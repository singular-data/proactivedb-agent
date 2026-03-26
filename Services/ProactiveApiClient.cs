using System.IO.Compression;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SingularData.Proactive.SqlMonitor.Service.Configuration;

namespace SingularData.Proactive.SqlMonitor.Service.Services;

/// <summary>
/// Sends collected SQL rows to the ProactiveDB Backend API ingestion endpoint.
/// Authenticated via X-Api-Key header (configured at HttpClient registration).
/// Each SQL row becomes one flat document in Elastic — optimal for Grafana queries.
/// </summary>
public sealed class ProactiveApiClient(
    HttpClient httpClient,
    ILogger<ProactiveApiClient> logger,
    IOptions<SqlMonitorOptions> options)
{
    private readonly string _databaseName =
        new SqlConnectionStringBuilder(options.Value.ConnectionString).InitialCatalog;

    // Matches TelemetryDataRequest on the BackendApi
    private sealed class TelemetryBatchItem
    {
        [JsonPropertyName("hostName")]
        public string HostName { get; init; } = string.Empty;

        [JsonPropertyName("tableName")]
        public string? TableName { get; init; }

        [JsonPropertyName("timestamp")]
        public DateTime? Timestamp { get; init; }

        // One item per request = one Elastic document per SQL row
        [JsonPropertyName("dataEntries")]
        public List<Dictionary<string, object?>>? DataEntries { get; init; }
    }

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNameCaseInsensitive = true
    };

    // -------------------------------------------------------------------------
    // Allowed-tables response shape (mirrors CollectedTableConfigResponse)
    // -------------------------------------------------------------------------
    private sealed class CollectedTableItem
    {
        [JsonPropertyName("schemaName")] public string SchemaName { get; init; } = string.Empty;
        [JsonPropertyName("tableName")]  public string TableName  { get; init; } = string.Empty;
        [JsonPropertyName("isEnabled")]  public bool   IsEnabled  { get; init; }
    }

    private sealed class PagedItems<T>
    {
        [JsonPropertyName("items")] public List<T> Items { get; init; } = [];
    }

    private sealed class ApiWrapper<T>
    {
        [JsonPropertyName("data")] public T? Data { get; init; }
    }

    public async Task<(bool Success, string? ErrorMessage)> SendBatchAsync(
        string tableName,
        List<Dictionary<string, object?>> rows,
        CancellationToken ct)
    {
        var hostName = Environment.MachineName;
        var now = DateTime.UtcNow;

        // One TelemetryBatchItem per row → one Elastic document per row
        var batch = rows.ConvertAll(row => new TelemetryBatchItem
        {
            HostName = hostName,
            TableName = tableName,
            Timestamp = now,
            DataEntries = [row]
        });

        try
        {
            var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(batch, JsonOpts);
            using var ms = new MemoryStream();
            await using (var gz = new GZipStream(ms, CompressionLevel.Fastest, leaveOpen: true))
                await gz.WriteAsync(jsonBytes, ct);

            logger.LogInformation(
                "[{Table}] Sending batch — rows: {Rows}, uncompressed: {Raw:F3} MB, compressed: {Gz:F3} MB",
                tableName, rows.Count, jsonBytes.Length / 1_048_576.0, ms.Length / 1_048_576.0);

            var content = new ByteArrayContent(ms.ToArray());
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            content.Headers.ContentEncoding.Add("gzip");

            var stopwatch = Stopwatch.StartNew();
            using var response = await httpClient.PostAsync("/api/ingestion/batch", content, ct);
            stopwatch.Stop();

            var elapsedMs = stopwatch.Elapsed.TotalMilliseconds;

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation(
                    "Batch of {Count} rows from [{Table}] accepted by API — status={Status} elapsedMs={ElapsedMs:F0}",
                    rows.Count, tableName, (int)response.StatusCode, elapsedMs);
                return (true, null);
            }

            var body = await response.Content.ReadAsStringAsync(ct);
            logger.LogWarning(
                "API returned {Status} for [{Table}] after {ElapsedMs:F0} ms: {Body}",
                (int)response.StatusCode, tableName, elapsedMs, body);
            return (false, $"HTTP {(int)response.StatusCode}: {Truncate(body, 200)}");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Walk the inner exception chain looking for an SSL/auth failure
            var authEx = ex.InnerException as System.Security.Authentication.AuthenticationException
                      ?? ex.InnerException?.InnerException as System.Security.Authentication.AuthenticationException;

            if (authEx is not null)
                logger.LogError(ex,
                    "SSL error sending batch for [{Table}] on machine {Machine} to {Url} — {SslMessage}",
                    tableName, hostName, httpClient.BaseAddress, authEx.Message);
            else
                logger.LogError(ex,
                    "HTTP error sending batch for [{Table}] on machine {Machine} to {Url}",
                    tableName, hostName, httpClient.BaseAddress);

            return (false, ex.Message);
        }
    }

    public async Task RegisterHostAsync(HostRegistrationPayload payload, CancellationToken ct)
    {
        try
        {
            var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(payload, JsonOpts);
            var content = new ByteArrayContent(jsonBytes);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            using var response = await httpClient.PostAsync("/api/hosts/register", content, ct);

            if (response.IsSuccessStatusCode)
            {
                var isNew = response.StatusCode == System.Net.HttpStatusCode.Created;
                logger.LogInformation(
                    "Host '{Hostname}' {Action} on backend",
                    payload.Hostname, isNew ? "registered (new)" : "updated (existing)");
            }
            else
            {
                var body = await response.Content.ReadAsStringAsync(ct);
                logger.LogWarning(
                    "Host registration returned {Status} for '{Hostname}': {Body}",
                    (int)response.StatusCode, payload.Hostname, Truncate(body, 200));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "Host registration failed for '{Hostname}' — continuing", payload.Hostname);
        }
    }

    public async Task SendEmailAlertAsync(string subject, string body, CancellationToken ct)
    {
        try
        {
            var payload = new { subject, body, isHtml = false };
            var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(payload, JsonOpts);

            var content = new ByteArrayContent(jsonBytes);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            using var response = await httpClient.PostAsync("/api/email/send", content, ct);

            if (response.IsSuccessStatusCode)
                logger.LogInformation("Email alert sent: {Subject}", subject);
            else
            {
                var responseBody = await response.Content.ReadAsStringAsync(ct);
                logger.LogWarning(
                    "Email alert failed with {Status}: {Body}",
                    (int)response.StatusCode, Truncate(responseBody, 200));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex, "Failed to send email alert");
        }
    }

    /// <summary>
    /// Returns the set of "schema.table" names that are enabled in the Backend API.
    /// Returns <c>null</c> if the API is unreachable or returns an error,
    /// so the caller can fall back to allowing all configured tables.
    /// </summary>
    public async Task<HashSet<string>?> GetEnabledTablesAsync(CancellationToken ct)
    {
        try
        {
            using var response = await httpClient.GetAsync(
                "/api/collected-tables?page=1&pageSize=1000", ct);

            if (!response.IsSuccessStatusCode)
            {
                logger.LogWarning(
                    "GetEnabledTables returned {Status} — proceeding with all configured tables",
                    (int)response.StatusCode);
                return null;
            }

            await using var stream = await response.Content.ReadAsStreamAsync(ct);
            var envelope = await JsonSerializer.DeserializeAsync<
                ApiWrapper<PagedItems<CollectedTableItem>>>(stream, JsonOpts, ct);

            if (envelope?.Data?.Items is null)
                return null;

            return envelope.Data.Items
                .Where(t => t.IsEnabled)
                .Select(t => $"{t.SchemaName}.{t.TableName}")
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // HttpClient's internal timeout fired (not the cycle-level CTS).
            // Treat as API unavailable and fall back to all configured tables.
            logger.LogWarning(
                "GetEnabledTables timed out (HttpClient timeout) — proceeding with all configured tables");
            return null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex,
                "Failed to fetch allowed tables from API — proceeding with all configured tables");
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // Agent registration & heartbeat
    // -------------------------------------------------------------------------

    private sealed class AgentRegistrationPayload
    {
        [JsonPropertyName("hostName")]    public string HostName    { get; init; } = string.Empty;
        [JsonPropertyName("serviceName")] public string ServiceName { get; init; } = "SqlMonitor";
        [JsonPropertyName("agentVersion")] public string? AgentVersion { get; init; }
        [JsonPropertyName("osInfo")]      public string? OsInfo     { get; init; }
    }

    private sealed class AgentRegistrationEnvelope
    {
        [JsonPropertyName("data")] public AgentRegistrationData? Data { get; init; }
    }

    private sealed class AgentRegistrationData
    {
        [JsonPropertyName("agentId")] public Guid AgentId { get; init; }
    }

    /// <summary>
    /// Registers this service as an agent on the backend and returns the assigned AgentId.
    /// Returns <c>null</c> if registration fails (non-fatal — heartbeats will be skipped until it succeeds).
    /// </summary>
    public async Task<Guid?> RegisterAgentAsync(CancellationToken ct)
    {
        try
        {
            var payload = new AgentRegistrationPayload
            {
                HostName    = Environment.MachineName,
                ServiceName = "SqlMonitor",
                OsInfo      = System.Runtime.InteropServices.RuntimeInformation.OSDescription
            };

            var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(payload, JsonOpts);
            var content   = new ByteArrayContent(jsonBytes);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            using var response = await httpClient.PostAsync("/api/agents/register", content, ct);

            if (!response.IsSuccessStatusCode)
            {
                var body = await response.Content.ReadAsStringAsync(ct);
                logger.LogWarning("Agent registration returned {Status}: {Body}",
                    (int)response.StatusCode, Truncate(body, 200));
                return null;
            }

            await using var stream = await response.Content.ReadAsStreamAsync(ct);
            var envelope = await JsonSerializer.DeserializeAsync<AgentRegistrationEnvelope>(stream, JsonOpts, ct);
            var agentId  = envelope?.Data?.AgentId;

            if (agentId is null || agentId == Guid.Empty)
            {
                logger.LogWarning("Agent registration succeeded but returned no AgentId");
                return null;
            }

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Agent registered — AgentId: {AgentId}", agentId);
            return agentId;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "Agent registration failed — heartbeats will be skipped");
            return null;
        }
    }

    /// <summary>
    /// Sends a rich Datadog-style heartbeat to the backend for the given agent.
    /// Non-fatal — failures are logged but never propagate.
    /// </summary>
    public async Task SendHeartbeatAsync(Guid agentId, AgentHealthSummary summary, CancellationToken ct)
    {
        try
        {
            var proc = System.Diagnostics.Process.GetCurrentProcess();
            var memMb = proc.WorkingSet64 / 1_048_576.0;

            var collectedAt = DateTime.UtcNow;
            var payload = new
            {
                collectedAtUtc     = collectedAt.ToString("o"),
                databaseCollection = _databaseName,
                memoryMb           = Math.Round(memMb, 1),
                activeQueries      = summary.ChecksTotal,
                statusMessage      = summary.StatusMessage,
                uptimeSeconds      = summary.UptimeSeconds,
                checksTotal        = summary.ChecksTotal,
                checksOk           = summary.ChecksOk,
                checksWarning      = summary.ChecksWarning,
                checksError        = summary.ChecksError,
                overallStatus      = summary.OverallStatus,
                checks             = summary.Checks
            };

            var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(payload, JsonOpts);
            var content   = new ByteArrayContent(jsonBytes);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            using var response = await httpClient.PostAsync(
                $"/api/agents/{agentId}/heartbeat", content, ct);

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation(
                    "Heartbeat sent — status={Status} ok={Ok} warn={Warn} err={Err}",
                    summary.OverallStatus, summary.ChecksOk, summary.ChecksWarning, summary.ChecksError);
            }
            else
            {
                var body = await response.Content.ReadAsStringAsync(ct);
                logger.LogWarning("Heartbeat returned {Status}: {Body}",
                    (int)response.StatusCode, Truncate(body, 200));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "Heartbeat failed — continuing");
        }
    }

    /// <summary>
    /// Sends SQL Server instance-level health snapshot to the backend for Elasticsearch indexing.
    /// Non-fatal — failures are logged but never propagate.
    /// </summary>
    public async Task SendInstanceHealthAsync(
        Guid agentId, SqlInstanceHealthSnapshot snapshot, CancellationToken ct)
    {
        try
        {
            var payload = new
            {
                collectedAtUtc         = snapshot.CollectedAtUtc.ToString("o"),
                databaseCollection     = _databaseName,
                serverName             = snapshot.ServerName,
                isAvailable            = snapshot.IsAvailable,
                errorMessage           = snapshot.ErrorMessage,
                sqlVersion             = snapshot.SqlVersion,
                uptimeSeconds          = snapshot.UptimeSeconds,
                cpuCount               = snapshot.CpuCount,
                sessionsTotal          = snapshot.SessionsTotal,
                sessionsActive         = snapshot.SessionsActive,
                sessionsBlocked        = snapshot.SessionsBlocked,
                memoryInUseMb          = snapshot.MemoryInUseMb,
                memoryUtilizationPct   = snapshot.MemoryUtilizationPct,
                targetServerMemoryMb   = snapshot.TargetServerMemoryMb,
                totalServerMemoryMb    = snapshot.TotalServerMemoryMb,
                batchRequestsTotal     = snapshot.BatchRequestsTotal,
                sqlCompilationsTotal   = snapshot.SqlCompilationsTotal,
                sqlRecompilationsTotal = snapshot.SqlRecompilationsTotal,
                bufferCacheHitRatioPct = snapshot.BufferCacheHitRatioPct,
                pageReadsTotal         = snapshot.PageReadsTotal,
                pageWritesTotal        = snapshot.PageWritesTotal,
                lockWaitsTotal         = snapshot.LockWaitsTotal,
                userConnections        = snapshot.UserConnections,
                databases              = snapshot.Databases?.Select(d => new
                {
                    name          = d.Name,
                    status        = d.Status,
                    recoveryModel = d.RecoveryModel,
                    logReuseWait  = d.LogReuseWait
                }),
                topWaits = snapshot.TopWaits?.Select(w => new
                {
                    waitType          = w.WaitType,
                    waitTimeMs        = w.WaitTimeMs,
                    waitingTasksCount = w.WaitingTasksCount,
                    signalWaitTimeMs  = w.SignalWaitTimeMs
                })
            };

            var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(payload, JsonOpts);
            var content   = new ByteArrayContent(jsonBytes);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            using var response = await httpClient.PostAsync(
                $"/api/agents/{agentId}/instance-health", content, ct);

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation(
                    "SQL instance health sent — available={Available} sessions={Sessions} blocked={Blocked}",
                    snapshot.IsAvailable, snapshot.SessionsTotal, snapshot.SessionsBlocked);
            }
            else
            {
                var body = await response.Content.ReadAsStringAsync(ct);
                logger.LogWarning("Instance health returned {Status}: {Body}",
                    (int)response.StatusCode, Truncate(body, 200));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "Failed to send SQL instance health — continuing");
        }
    }

    /// <summary>
    /// Posts one document per SQL-related OS service to POST /api/agents/{agentId}/os-services.
    /// Non-fatal — failures are logged but never propagate.
    /// </summary>
    public async Task SendOsServicesAsync(
        Guid agentId, OsServicesSnapshot snapshot, CancellationToken ct)
    {
        var ts        = snapshot.CollectedAtUtc.ToString("o");
        int sent      = 0;
        int failures  = 0;

        foreach (var svc in snapshot.Services)
        {
            try
            {
                var payload = new
                {
                    collectedAtUtc     = ts,
                    databaseCollection = _databaseName,
                    isAvailable        = snapshot.IsAvailable,
                    errorMessage       = snapshot.ErrorMessage,
                    osPlatform         = snapshot.OsPlatform,
                    name               = svc.Name,
                    displayName        = svc.DisplayName,
                    state              = svc.State,
                    startMode          = svc.StartMode,
                    processId          = svc.ProcessId,
                    port               = svc.Port,
                    isSqlEngineService = svc.IsSqlEngineService,
                    isMainInstance     = svc.IsMainInstance,
                    isAlertCondition   = svc.IsAlertCondition
                };

                var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(payload, JsonOpts);
                var content   = new ByteArrayContent(jsonBytes);
                content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

                using var response = await httpClient.PostAsync(
                    $"/api/agents/{agentId}/os-services", content, ct);

                if (response.IsSuccessStatusCode)
                    sent++;
                else
                {
                    var body = await response.Content.ReadAsStringAsync(ct);
                    logger.LogWarning("OS service [{Name}] returned {Status}: {Body}",
                        svc.Name, (int)response.StatusCode, Truncate(body, 200));
                    failures++;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Failed to send OS service [{Name}] — continuing", svc.Name);
                failures++;
            }
        }

        if (failures == 0)
            logger.LogInformation(
                "OS services sent — platform={Platform} total={Total} sent={Sent}",
                snapshot.OsPlatform, snapshot.Services.Count, sent);
        else
            logger.LogWarning(
                "OS services partially sent — platform={Platform} total={Total} sent={Sent} failures={Failures}",
                snapshot.OsPlatform, snapshot.Services.Count, sent, failures);
    }

    private static string Truncate(string? s, int max) =>
        s is null ? "" : s.Length <= max ? s : s[..max] + "…";
}
