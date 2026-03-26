using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SingularData.Proactive.SqlMonitor.Service.Configuration;

namespace SingularData.Proactive.SqlMonitor.Service.Services;

/// <summary>
/// Collects SQL Server instance-level health metrics via DMVs on each cycle.
/// Requires VIEW SERVER STATE on the monitoring login — no sysadmin needed.
/// All queries use CommandTimeout = 10s and NOLOCK where applicable to avoid
/// impacting the monitored server.
/// </summary>
public sealed class SqlInstanceCollector(
    ILogger<SqlInstanceCollector> logger,
    IOptions<SqlMonitorOptions> options)
{
    // Idle wait types that add noise — same exclusion list as Datadog's SQL Server check
    private static readonly HashSet<string> IdleWaitTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "SLEEP_TASK", "WAITFOR", "LAZYWRITER_SLEEP", "SQLTRACE_BUFFER_FLUSH",
        "SQLTRACE_INCREMENTAL_FLUSH_SLEEP", "LOGMGR_QUEUE", "CHECKPOINT_QUEUE",
        "REQUEST_FOR_DEADLOCK_SEARCH", "XE_TIMER_EVENT", "XE_DISPATCHER_WAIT",
        "BROKER_TO_FLUSH", "BROKER_TASK_STOP", "CLR_AUTO_EVENT",
        "DISPATCHER_QUEUE_SEMAPHORE", "ONDEMAND_TASK_QUEUE", "RESOURCE_QUEUE",
        "SERVER_IDLE_CHECK", "SLEEP_DBSTARTUP", "SLEEP_DCOMSTARTUP",
        "SLEEP_MASTERDBREADY", "SLEEP_MASTERMDREADY", "SLEEP_MASTERUPGRADED",
        "SLEEP_MSDBSTARTUP", "SLEEP_SYSTEMTASK", "SLEEP_TEMPDBSTARTUP",
        "SNI_HTTP_ACCEPT", "SP_SERVER_DIAGNOSTICS_SLEEP", "HADR_WORK_QUEUE",
        "HADR_FILESTREAM_IOMGR_IOCOMPLETION", "FT_IFTS_SCHEDULER_IDLE_WAIT",
        "BROKER_EVENTHANDLER", "DBMIRROR_EVENTS_QUEUE", "SQLTRACE_WAIT_ENTRIES",
        "WAIT_XTP_OFFLINE_CKPT_NEW_LOG", "XIO_IDLE"
    };

    public async Task<SqlInstanceHealthSnapshot> CollectAsync(CancellationToken ct)
    {
        var cs = new SqlConnectionStringBuilder(options.Value.ConnectionString);
        var snapshot = new SqlInstanceHealthSnapshot { ServerName = cs.DataSource };

        try
        {
            await using var conn = new SqlConnection(options.Value.ConnectionString);
            await conn.OpenAsync(ct);
            snapshot.IsAvailable = true;

            await CollectInstanceInfoAsync(conn, snapshot, ct);
            await CollectSessionsAsync(conn, snapshot, ct);
            await CollectMemoryAsync(conn, snapshot, ct);
            await CollectPerfCountersAsync(conn, snapshot, ct);
            await CollectDatabasesAsync(conn, snapshot, ct);
            await CollectWaitStatsAsync(conn, snapshot, ct);

            logger.LogInformation(
                "SQL instance health collected — server={Server} sessions={Sessions} blocked={Blocked}",
                snapshot.ServerName, snapshot.SessionsTotal, snapshot.SessionsBlocked);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            snapshot.IsAvailable = false;
            snapshot.ErrorMessage = $"{ex.GetType().Name}: {ex.Message}";
            logger.LogWarning(ex,
                "SQL instance health collection failed for {Server}", cs.DataSource);
        }

        return snapshot;
    }

    // -------------------------------------------------------------------------

    private static async Task CollectInstanceInfoAsync(
        SqlConnection conn, SqlInstanceHealthSnapshot snap, CancellationToken ct)
    {
        const string sql = """
            SELECT
                @@SERVERNAME                                                    AS server_name,
                @@VERSION                                                       AS sql_version,
                DATEDIFF(SECOND, sqlserver_start_time, GETUTCDATE())           AS uptime_seconds,
                cpu_count
            FROM sys.dm_os_sys_info;
            """;

        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 10 };
        await using var r = await cmd.ExecuteReaderAsync(ct);
        if (await r.ReadAsync(ct))
        {
            snap.ServerName    = r["server_name"] as string ?? snap.ServerName;
            snap.SqlVersion    = (r["sql_version"] as string)?.Split('\n')[0].Trim();
            snap.UptimeSeconds = r["uptime_seconds"] as long? ?? Convert.ToInt64(r["uptime_seconds"]);
            snap.CpuCount      = r["cpu_count"] as int? ?? Convert.ToInt32(r["cpu_count"]);
        }
    }

    private static async Task CollectSessionsAsync(
        SqlConnection conn, SqlInstanceHealthSnapshot snap, CancellationToken ct)
    {
        const string sql = """
            SELECT
                COUNT(*)                                                                      AS total_sessions,
                SUM(CASE WHEN r.session_id IS NOT NULL THEN 1 ELSE 0 END)                    AS active_requests,
                SUM(CASE WHEN r.blocking_session_id > 0 THEN 1 ELSE 0 END)                   AS blocked_sessions
            FROM sys.dm_exec_sessions s
            LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
            WHERE s.is_user_process = 1;
            """;

        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 10 };
        await using var r = await cmd.ExecuteReaderAsync(ct);
        if (await r.ReadAsync(ct))
        {
            snap.SessionsTotal   = Convert.ToInt32(r["total_sessions"]);
            snap.SessionsActive  = Convert.ToInt32(r["active_requests"]);
            snap.SessionsBlocked = Convert.ToInt32(r["blocked_sessions"]);
        }
    }

    private static async Task CollectMemoryAsync(
        SqlConnection conn, SqlInstanceHealthSnapshot snap, CancellationToken ct)
    {
        const string sql = """
            SELECT
                physical_memory_in_use_kb / 1024   AS memory_in_use_mb,
                memory_utilization_percentage       AS memory_utilization_pct
            FROM sys.dm_os_process_memory;
            """;

        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 10 };
        await using var r = await cmd.ExecuteReaderAsync(ct);
        if (await r.ReadAsync(ct))
        {
            snap.MemoryInUseMb        = Convert.ToInt64(r["memory_in_use_mb"]);
            snap.MemoryUtilizationPct = Convert.ToInt32(r["memory_utilization_pct"]);
        }
    }

    private static async Task CollectPerfCountersAsync(
        SqlConnection conn, SqlInstanceHealthSnapshot snap, CancellationToken ct)
    {
        const string sql = """
            SELECT counter_name, cntr_value
            FROM sys.dm_os_performance_counters
            WHERE instance_name IN ('', '_Total')
              AND counter_name IN (
                'Batch Requests/sec',
                'SQL Compilations/sec',
                'SQL Re-Compilations/sec',
                'Page reads/sec',
                'Page writes/sec',
                'Lock Waits/sec',
                'User Connections',
                'Buffer cache hit ratio',
                'Buffer cache hit ratio base',
                'Target Server Memory (KB)',
                'Total Server Memory (KB)'
              );
            """;

        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 10 };
        await using var r = await cmd.ExecuteReaderAsync(ct);

        var counters = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        while (await r.ReadAsync(ct))
        {
            var name  = (r["counter_name"] as string ?? "").Trim();
            var value = Convert.ToInt64(r["cntr_value"]);
            counters[name] = value;
        }

        snap.BatchRequestsTotal     = counters.GetValueOrDefault("Batch Requests/sec");
        snap.SqlCompilationsTotal   = counters.GetValueOrDefault("SQL Compilations/sec");
        snap.SqlRecompilationsTotal = counters.GetValueOrDefault("SQL Re-Compilations/sec");
        snap.PageReadsTotal         = counters.GetValueOrDefault("Page reads/sec");
        snap.PageWritesTotal        = counters.GetValueOrDefault("Page writes/sec");
        snap.LockWaitsTotal         = counters.GetValueOrDefault("Lock Waits/sec");
        snap.UserConnections        = counters.GetValueOrDefault("User Connections");

        var targetKb = counters.GetValueOrDefault("Target Server Memory (KB)");
        var totalKb  = counters.GetValueOrDefault("Total Server Memory (KB)");
        if (targetKb > 0) snap.TargetServerMemoryMb = targetKb / 1024;
        if (totalKb  > 0) snap.TotalServerMemoryMb  = totalKb  / 1024;

        // Buffer cache hit ratio = numerator / base × 100
        var numerator = counters.GetValueOrDefault("Buffer cache hit ratio");
        var @base     = counters.GetValueOrDefault("Buffer cache hit ratio base");
        if (@base > 0)
            snap.BufferCacheHitRatioPct = Math.Round((double)numerator / @base * 100, 2);
    }

    private static async Task CollectDatabasesAsync(
        SqlConnection conn, SqlInstanceHealthSnapshot snap, CancellationToken ct)
    {
        const string sql = """
            SELECT name, state_desc, recovery_model_desc, log_reuse_wait_desc
            FROM sys.databases WITH (NOLOCK)
            ORDER BY name;
            """;

        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 10 };
        await using var r = await cmd.ExecuteReaderAsync(ct);

        snap.Databases = [];
        while (await r.ReadAsync(ct))
        {
            snap.Databases.Add(new SqlDatabaseStatus
            {
                Name          = r["name"] as string ?? "",
                Status        = r["state_desc"] as string ?? "",
                RecoveryModel = r["recovery_model_desc"] as string,
                LogReuseWait  = r["log_reuse_wait_desc"] as string
            });
        }
    }

    private async Task CollectWaitStatsAsync(
        SqlConnection conn, SqlInstanceHealthSnapshot snap, CancellationToken ct)
    {
        const string sql = """
            SELECT TOP 10
                wait_type,
                wait_time_ms,
                waiting_tasks_count,
                signal_wait_time_ms
            FROM sys.dm_os_wait_stats
            WHERE waiting_tasks_count > 0
            ORDER BY wait_time_ms DESC;
            """;

        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 10 };
        await using var r = await cmd.ExecuteReaderAsync(ct);

        snap.TopWaits = [];
        while (await r.ReadAsync(ct))
        {
            var waitType = r["wait_type"] as string ?? "";
            if (IdleWaitTypes.Contains(waitType)) continue;

            snap.TopWaits.Add(new SqlWaitStat
            {
                WaitType          = waitType,
                WaitTimeMs        = Convert.ToInt64(r["wait_time_ms"]),
                WaitingTasksCount = Convert.ToInt64(r["waiting_tasks_count"]),
                SignalWaitTimeMs  = Convert.ToInt64(r["signal_wait_time_ms"])
            });
        }
    }
}

// -------------------------------------------------------------------------
// Local snapshot types — serialized and sent to the backend API
// -------------------------------------------------------------------------

public sealed class SqlInstanceHealthSnapshot
{
    public DateTime CollectedAtUtc        { get; set; } = DateTime.UtcNow;
    public string  ServerName            { get; set; } = string.Empty;
    public bool    IsAvailable           { get; set; }
    public string? ErrorMessage          { get; set; }
    public string? SqlVersion            { get; set; }
    public long?   UptimeSeconds         { get; set; }
    public int?    CpuCount              { get; set; }
    public int?    SessionsTotal         { get; set; }
    public int?    SessionsActive        { get; set; }
    public int?    SessionsBlocked       { get; set; }
    public long?   MemoryInUseMb         { get; set; }
    public int?    MemoryUtilizationPct  { get; set; }
    public long?   TargetServerMemoryMb  { get; set; }
    public long?   TotalServerMemoryMb   { get; set; }
    public long?   BatchRequestsTotal    { get; set; }
    public long?   SqlCompilationsTotal  { get; set; }
    public long?   SqlRecompilationsTotal{ get; set; }
    public double? BufferCacheHitRatioPct{ get; set; }
    public long?   PageReadsTotal        { get; set; }
    public long?   PageWritesTotal       { get; set; }
    public long?   LockWaitsTotal        { get; set; }
    public long?   UserConnections       { get; set; }
    public List<SqlDatabaseStatus>? Databases { get; set; }
    public List<SqlWaitStat>?       TopWaits  { get; set; }
}

public sealed class SqlDatabaseStatus
{
    public string  Name          { get; set; } = string.Empty;
    public string  Status        { get; set; } = string.Empty;
    public string? RecoveryModel { get; set; }
    public string? LogReuseWait  { get; set; }
}

public sealed class SqlWaitStat
{
    public string WaitType          { get; set; } = string.Empty;
    public long   WaitTimeMs        { get; set; }
    public long   WaitingTasksCount { get; set; }
    public long   SignalWaitTimeMs  { get; set; }
}
