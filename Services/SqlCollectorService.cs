using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SingularData.Proactive.SqlMonitor.Service.Configuration;

namespace SingularData.Proactive.SqlMonitor.Service.Services;

/// <summary>
/// For each configured SQL Server instance, and for each configured table within that instance:
///   1. SELECT TOP N unsent rows (sent_monitoring IS NULL OR = 0)
///   2. POST them as a JSON batch to the Backend API
///   3. UPDATE sent_monitoring = 1/0 and sent_monitoring_desc based on API response
/// Instances run concurrently (Task.WhenAll or Parallel.ForEachAsync bounded by MaxParallelInstances).
/// Tables within each instance run concurrently (up to MaxParallelTables concurrent).
/// Results are reported to AgentHealthTracker for Datadog-style check status.
/// </summary>
public sealed class SqlCollectorService(
    ILogger<SqlCollectorService> logger,
    IOptions<SqlMonitorOptions> options,
    ProactiveApiClient apiClient,
    AgentHealthTracker healthTracker)
{
    // -------------------------------------------------------------------------
    // Allowed-tables cache (5-minute TTL)
    // -------------------------------------------------------------------------
    private static readonly TimeSpan CacheTtl = TimeSpan.FromMinutes(5);
    private HashSet<string>? _allowedTablesCache;
    private DateTime _cacheExpiresAt = DateTime.MinValue;
    private readonly SemaphoreSlim _cacheLock = new(1, 1);

    private async Task<HashSet<string>?> GetAllowedTablesAsync(CancellationToken ct)
    {
        // Fast path — still valid
        if (DateTime.UtcNow < _cacheExpiresAt && _allowedTablesCache is not null)
            return _allowedTablesCache;

        await _cacheLock.WaitAsync(ct);
        try
        {
            // Double-check after acquiring the lock
            if (DateTime.UtcNow < _cacheExpiresAt && _allowedTablesCache is not null)
                return _allowedTablesCache;

            var fetched = await apiClient.GetEnabledTablesAsync(ct);
            if (fetched is not null)
            {
                _allowedTablesCache = fetched;
                _cacheExpiresAt = DateTime.UtcNow.Add(CacheTtl);
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "Allowed tables cache refreshed — {Count} table(s) enabled in API",
                        fetched.Count);
            }
            else
            {
                // API unavailable: keep existing cache if present, otherwise null (fail-open)
                var fallback = _allowedTablesCache is not null
                    ? "using previous cached list"
                    : "proceeding with all configured tables";
                logger.LogWarning(
                    "Could not refresh allowed tables cache — {Fallback}", fallback);
            }

            return _allowedTablesCache;
        }
        finally
        {
            _cacheLock.Release();
        }
    }

    // -------------------------------------------------------------------------
    // Instance resolution — supports both new Instances[] and legacy flat config
    // -------------------------------------------------------------------------

    private static List<SqlInstanceConfig> ResolveInstances(SqlMonitorOptions opts)
    {
        if (opts.Instances.Count > 0)
            return opts.Instances;

        if (!string.IsNullOrWhiteSpace(opts.ConnectionString))
        {
            var cs = new SqlConnectionStringBuilder(opts.ConnectionString);
            return [new SqlInstanceConfig
            {
                Name             = !string.IsNullOrWhiteSpace(cs.DataSource) ? cs.DataSource : "default",
                ConnectionString = opts.ConnectionString,
                Tables           = opts.Tables
            }];
        }

        return [];
    }

    // -------------------------------------------------------------------------

    public async Task RunCycleAsync(CancellationToken ct)
    {
        var opts      = options.Value;
        var instances = ResolveInstances(opts);

        if (instances.Count == 0)
        {
            logger.LogWarning("No SQL instances configured — skipping cycle");
            return;
        }

        // Fetch the API's enabled-table list once for all instances (cached 5 min)
        logger.LogInformation("Fetching allowed tables from API/cache");
        var allowedTables = await GetAllowedTablesAsync(ct);

        if (allowedTables is null)
        {
            logger.LogWarning("Allowed tables unavailable from API/cache — proceeding with all configured tables");
        }
        else
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "Allowed tables resolved from API/cache — {Count} table(s) enabled",
                    allowedTables.Count);
        }

        if (opts.MaxParallelInstances <= 0)
        {
            await Task.WhenAll(instances.Select(inst =>
                RunInstanceCycleAsync(inst, allowedTables, opts, ct)));
        }
        else
        {
            await Parallel.ForEachAsync(
                instances,
                new ParallelOptions { MaxDegreeOfParallelism = opts.MaxParallelInstances, CancellationToken = ct },
                async (inst, token) => await RunInstanceCycleAsync(inst, allowedTables, opts, token));
        }
    }

    // -------------------------------------------------------------------------

    private async Task RunInstanceCycleAsync(
        SqlInstanceConfig instance,
        HashSet<string>?  allowedTables,
        SqlMonitorOptions opts,
        CancellationToken ct)
    {
        List<TableMonitorConfig> tablesToProcess;

        var useWildcard = instance.Tables.Count == 0
            || instance.Tables.Any(t => t.TableName is null or "" or "*");

        if (!useWildcard)
        {
            // Explicit table list — filter by API allowlist when available
            if (allowedTables is null)
            {
                tablesToProcess = instance.Tables;
            }
            else
            {
                tablesToProcess = [.. instance.Tables.Where(t => allowedTables.Contains(t.TableName))];

                var skipped = instance.Tables.Count - tablesToProcess.Count;
                if (skipped > 0 && logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "[{Instance}] {Skipped} table(s) skipped — not enabled in API",
                        instance.Name, skipped);
            }
        }
        else
        {
            // No tables configured (or wildcard "*") — discover from the API enabled-tables list
            if (allowedTables is null)
            {
                logger.LogWarning(
                    "[{Instance}] No tables configured and API table list unavailable — skipping instance",
                    instance.Name);
                return;
            }

            tablesToProcess = [.. allowedTables.Select(t => new TableMonitorConfig { TableName = t })];

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "[{Instance}] Tables=* — auto-discovered {Count} table(s) from API",
                    instance.Name, tablesToProcess.Count);
        }

        if (tablesToProcess.Count == 0)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("[{Instance}] No tables to process this cycle", instance.Name);
            return;
        }

        var maxParallelTables = instance.MaxParallelTables ?? opts.MaxParallelTables;

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation(
                "[{Instance}] Processing {Count} table(s) with MaxParallelTables={Parallel}",
                instance.Name, tablesToProcess.Count, maxParallelTables);

        var errors = new ConcurrentBag<(string Subject, string Detail)>();

        await Parallel.ForEachAsync(
            tablesToProcess,
            new ParallelOptions { MaxDegreeOfParallelism = maxParallelTables, CancellationToken = ct },
            async (table, token) => await ProcessTableAsync(instance, table, opts, errors, token));

        if (!errors.IsEmpty)
            await SendConsolidatedAlertAsync(errors, ct);
    }

    // Columns managed internally for monitoring state — never forwarded to the API
    private static readonly HashSet<string> InternalColumns =
        new(["sent_monitoring", "sent_monitoring_desc"], StringComparer.OrdinalIgnoreCase);

    // -------------------------------------------------------------------------

    private async Task ProcessTableAsync(
        SqlInstanceConfig instance,
        TableMonitorConfig table,
        SqlMonitorOptions opts,
        ConcurrentBag<(string Subject, string Detail)> errors,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(table.TableName))
        {
            logger.LogWarning("[{Instance}] A table config entry has no TableName — skipping", instance.Name);
            return;
        }

        var sw    = Stopwatch.StartNew();
        var stage = "initializing";

        try
        {
            var cs = new SqlConnectionStringBuilder(instance.ConnectionString);

            var keyCol   = string.IsNullOrWhiteSpace(table.KeyColumn) ? "id" : table.KeyColumn.Trim();
            var fmtTable = FormatTableName(table.TableName);
            var maxRows  = instance.MaxRowsPerTable ?? opts.MaxRowsPerTable;

            // If Columns is omitted or empty, use SELECT * and strip internal columns at read time
            var explicitColumns = string.IsNullOrWhiteSpace(table.Columns)
                ? null
                : table.Columns
                    .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                    .Where(c => !InternalColumns.Contains(c))
                    .ToArray();

            var colSql = explicitColumns is { Length: > 0 }
                ? string.Join(", ", explicitColumns.Select(c => $"[{c}]"))
                : "*";

            var query = $"""
                SELECT TOP ({maxRows}) {colSql}
                FROM {fmtTable}
                WHERE sent_monitoring IS NULL OR sent_monitoring = 0
                """;

            var rows      = new List<Dictionary<string, object?>>();
            var keyValues = new List<object>();

            await using var conn = new SqlConnection(instance.ConnectionString);

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("[{Instance}] [{Table}] Processing started", instance.Name, table.TableName);

            // 1. Connection failure
            try
            {
                stage = "opening SQL connection";
                await conn.OpenAsync(ct);
            }
            catch (SqlException ex)
            {
                errors.Add((
                    $"[ProactiveDB] SQL connection failed — {instance.Name}/{cs.DataSource}",
                    $"SQL Server connection failure on {Environment.MachineName}\n" +
                    $"Time     : {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC\n" +
                    $"Instance : {instance.Name}\n" +
                    $"Server   : {cs.DataSource}\n" +
                    $"Database : {cs.InitialCatalog}\n" +
                    $"Table    : {table.TableName}\n\n" +
                    $"Error [{ex.Number}]: {ex.Message}"));
                throw;
            }

            // 2. SELECT query failure
            try
            {
                stage = "executing SELECT query";
                await using var cmd    = new SqlCommand(query, conn) { CommandTimeout = 30 };
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                while (await reader.ReadAsync(ct))
                {
                    var row = new Dictionary<string, object?>(
                        reader.FieldCount, StringComparer.OrdinalIgnoreCase);

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var colName = reader.GetName(i);
                        if (InternalColumns.Contains(colName)) continue;
                        row[colName] = reader.IsDBNull(i) ? null : ConvertValue(reader.GetValue(i));
                    }

                    rows.Add(row);

                    if (!string.IsNullOrWhiteSpace(keyCol)
                        && row.TryGetValue(keyCol, out var kv) && kv is not null)
                        keyValues.Add(kv);
                }
            }
            catch (SqlException ex)
            {
                errors.Add((
                    $"[ProactiveDB] SQL query failed — {instance.Name}/{table.TableName}",
                    $"SQL query execution failed on {Environment.MachineName}\n" +
                    $"Time     : {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC\n" +
                    $"Instance : {instance.Name}\n" +
                    $"Server   : {cs.DataSource}\n" +
                    $"Database : {cs.InitialCatalog}\n" +
                    $"Table    : {table.TableName}\n\n" +
                    $"Error [{ex.Number}]: {ex.Message}"));
                throw;
            }

            if (rows.Count == 0)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("[{Instance}] [{Table}] No unsent rows found", instance.Name, table.TableName);
                // Count as Ok — no rows simply means nothing to send this cycle
                healthTracker.RecordResult(instance.Name, table.TableName, success: true, rowsCollected: 0, sw.ElapsedMilliseconds);
                return;
            }

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("[{Instance}] [{Table}] Collected {Count} row(s)", instance.Name, table.TableName, rows.Count);

            stage = "sending batch to API";
            var (success, errorMsg) = await apiClient.SendBatchAsync(table.TableName, rows, ct);

            // Report health check result regardless of key-column state
            healthTracker.RecordResult(instance.Name, table.TableName, success, rows.Count, sw.ElapsedMilliseconds, errorMsg);

            if (!success && keyValues.Count > 0)
                logger.LogWarning(
                    "[{Instance}] [{Table}] Batch rejected — {KeyColumn} values in failed batch: [{Keys}]",
                    instance.Name, table.TableName, keyCol,
                    string.Join(", ", keyValues));

            if (string.IsNullOrWhiteSpace(keyCol))
            {
                logger.LogWarning(
                    "[{Instance}] [{Table}] KeyColumn not configured — rows cannot be marked as sent",
                    instance.Name, table.TableName);
                return;
            }
            if (keyValues.Count == 0)
            {
                logger.LogWarning(
                    "[{Instance}] [{Table}] No key values found in result (KeyColumn={Key})",
                    instance.Name, table.TableName, keyCol);
                return;
            }

            // 3. UPDATE failure handled inside
            stage = "updating sent_monitoring status";
            await UpdateSentStatusAsync(conn, fmtTable, instance, table.TableName, cs, keyCol, keyValues, success, errorMsg, errors, ct);

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "[{Instance}] [{Table}] Processing completed in {Ms}ms",
                    instance.Name, table.TableName, sw.ElapsedMilliseconds);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            logger.LogWarning(
                "[{Instance}] [{Table}] Processing cancelled during {Stage} after {Ms}ms",
                instance.Name, table.TableName, stage, sw.ElapsedMilliseconds);
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException and not SqlException)
        {
            // SqlException alerts are sent at the specific catch sites above; this
            // handles truly unexpected errors (NullReference, serialization, etc.)
            logger.LogError(ex, "[{Instance}] [{Table}] Unhandled error during processing", instance.Name, table.TableName);

            healthTracker.RecordResult(instance.Name, table.TableName, success: false, rowsCollected: 0,
                sw.ElapsedMilliseconds, $"{ex.GetType().Name}: {ex.Message}");

            errors.Add((
                $"[ProactiveDB] Unexpected error — {instance.Name}/{table.TableName}",
                $"Unexpected error during collection on {Environment.MachineName}\n" +
                $"Time     : {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC\n" +
                $"Instance : {instance.Name}\n" +
                $"Table    : {table.TableName}\n\n" +
                $"{ex.GetType().Name}: {ex.Message}\n\n{ex.StackTrace}"));
        }
        catch (SqlException ex)
        {
            // SqlException already alerted above — only log here to avoid duplicate emails
            logger.LogError(ex, "[{Instance}] [{Table}] Unhandled error during processing", instance.Name, table.TableName);

            healthTracker.RecordResult(instance.Name, table.TableName, success: false, rowsCollected: 0,
                sw.ElapsedMilliseconds, $"SQL [{ex.Number}]: {ex.Message}");
        }
    }

    private async Task UpdateSentStatusAsync(
        SqlConnection conn,
        string fmtTable,
        SqlInstanceConfig instance,
        string tableName,
        SqlConnectionStringBuilder cs,
        string keyCol,
        List<object> keyValues,
        bool success,
        string? errorMsg,
        ConcurrentBag<(string Subject, string Detail)> errors,
        CancellationToken ct)
    {
        var status = success ? 1 : 0;
        var desc   = "sent";

        try
        {
            var colType = keyValues[0].GetType();
            var sqlType = ToSqlTypeName(colType);

            await using (var createCmd = new SqlCommand(
                $"CREATE TABLE #TempIds (Id {sqlType} NOT NULL)", conn) { CommandTimeout = 30 })
                await createCmd.ExecuteNonQueryAsync(ct);

            var dt = new DataTable();
            dt.Columns.Add("Id", colType);
            foreach (var id in keyValues)
                dt.Rows.Add(id);

            using var bulk = new SqlBulkCopy(conn) { DestinationTableName = "#TempIds", BulkCopyTimeout = 60 };
            await bulk.WriteToServerAsync(dt, ct);

            if (!success && !string.IsNullOrWhiteSpace(errorMsg))
            {
                logger.LogWarning(
                    "[{Instance}] [{Table}] Failed to send batch to API: {Error}",
                    instance.Name, tableName, errorMsg);
            }

            var sql = success
                ? $"""
                    UPDATE t
                    SET t.sent_monitoring      = @status,
                        t.sent_monitoring_desc = @desc
                    FROM {fmtTable} t
                    INNER JOIN #TempIds i ON t.[{keyCol}] = i.Id
                    """
                : $"""
                    UPDATE t
                    SET t.sent_monitoring = @status
                    FROM {fmtTable} t
                    INNER JOIN #TempIds i ON t.[{keyCol}] = i.Id
                    """;

            await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 30 };
            cmd.Parameters.AddWithValue("@status", status);
            if (success)
                cmd.Parameters.AddWithValue("@desc", desc);

            var updated = await cmd.ExecuteNonQueryAsync(ct);
            if (logger.IsEnabled(LogLevel.Information))
            {
                var statusLabel = success ? "sent" : "failed";
                logger.LogInformation(
                    "[{Instance}] [{Table}] Marked {Updated}/{Total} row(s) as [{Status}]",
                    instance.Name, fmtTable, updated, keyValues.Count, statusLabel);
            }
        }
        catch (SqlException ex)
        {
            errors.Add((
                $"[ProactiveDB] Failed to mark rows — {instance.Name}/{tableName}",
                $"Failed to update sent_monitoring status on {Environment.MachineName}\n" +
                $"Time     : {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC\n" +
                $"Instance : {instance.Name}\n" +
                $"Server   : {cs.DataSource}\n" +
                $"Database : {cs.InitialCatalog}\n" +
                $"Table    : {tableName}\n" +
                $"Rows     : {keyValues.Count} row(s)\n\n" +
                $"Error [{ex.Number}]: {ex.Message}"));
            throw;
        }
    }

    private async Task SendConsolidatedAlertAsync(
        ConcurrentBag<(string Subject, string Detail)> errors,
        CancellationToken ct)
    {
        var items   = errors.ToArray();
        var subject = $"[ProactiveDB] {items.Length} collection error(s) on {Environment.MachineName}";
        var body    = string.Join(
            "\n\n" + new string('-', 60) + "\n\n",
            items.Select((e, i) => $"[{i + 1}/{items.Length}] {e.Subject}\n\n{e.Detail}"));

        await apiClient.SendEmailAlertAsync(subject, body, ct);
    }

    private static string ToSqlTypeName(Type type)
    {
        if (type == typeof(int))   return "INT";
        if (type == typeof(long))  return "BIGINT";
        if (type == typeof(short)) return "SMALLINT";
        if (type == typeof(Guid))  return "UNIQUEIDENTIFIER";
        return "NVARCHAR(450)";
    }

    // -------------------------------------------------------------------------
    // Helpers

    /// <summary>Wraps table name in brackets, supporting "schema.table" notation.</summary>
    private static string FormatTableName(string name)
    {
        var parts = name.Split('.', 2);
        return parts.Length == 2
            ? $"[{parts[0].Trim()}].[{parts[1].Trim()}]"
            : $"[{name.Trim()}]";
    }

    private static object? ConvertValue(object value) => value switch
    {
        DBNull => null,
        byte[] b => Convert.ToBase64String(b),
        _ => value
    };
}
