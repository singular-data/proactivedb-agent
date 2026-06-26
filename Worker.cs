using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SingularData.Proactive.SqlMonitor.Service.Configuration;
using SingularData.Proactive.SqlMonitor.Service.Services;

namespace SingularData.Proactive.SqlMonitor.Service;

/// <summary>
/// Background service that drives the collection cycle on a fixed interval.
/// Startup sequence:
///   1. Register host hardware info (upsert)
///   2. Register agent → obtain AgentId for heartbeats
///   3. Run collection cycle immediately
///   4. Send OS service status snapshot
///   5. Send heartbeat with per-table check status
///   6. Repeat from 3 every PollIntervalMinutes
/// </summary>
public sealed class Worker(
    ILogger<Worker> logger,
    IHostApplicationLifetime appLifetime,
    IOptions<SqlMonitorOptions> options,
    SqlCollectorService collector,
    OsServicesCollector osServicesCollector,
    ProactiveApiClient apiClient,
    AgentHealthTracker healthTracker) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var opts = options.Value;
        var proc = Process.GetCurrentProcess();

        // ── Startup banner ────────────────────────────────────────────────────
        if (logger.IsEnabled(LogLevel.Information))
        {
            var pid           = proc.Id;
            var machine       = Environment.MachineName;
            var user          = Environment.UserName;
            var runtime       = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription;
            var exePath       = proc.MainModule?.FileName ?? "unknown";
            var instanceCount = opts.Instances.Count > 0 ? opts.Instances.Count : 1;
            var totalTables   = opts.Instances.Count > 0
                ? opts.Instances.Sum(i => i.Tables.Count)
                : opts.Tables.Count;

            logger.LogInformation(
                "=== SQL Monitor starting — PID={Pid} Machine={Machine} User={User} Runtime={Runtime} Exe={Exe} ===",
                pid, machine, user, runtime, exePath);

            logger.LogInformation(
                "Config — interval={Interval}min maxRows={Max} maxParallelTables={Parallel} maxParallelInstances={MaxInst} instances={Instances} tables={Tables}",
                opts.PollIntervalMinutes, opts.MaxRowsPerTable, opts.MaxParallelTables,
                opts.MaxParallelInstances, instanceCount, totalTables);
        }

        // ── Lifecycle events (fire-and-forget callbacks from the host) ────────
        appLifetime.ApplicationStarted.Register(() =>
            logger.LogInformation("Host ApplicationStarted fired — service fully running"));

        appLifetime.ApplicationStopping.Register(() =>
            logger.LogWarning("Host ApplicationStopping fired — graceful shutdown initiated"));

        appLifetime.ApplicationStopped.Register(() =>
            logger.LogWarning("Host ApplicationStopped fired — process is about to exit"));

        var cycleNumber = 0;

        try
        {
            // 1. Register host hardware (upsert — non-fatal)
            try
            {
                var hostInfo = HostInfoCollector.Collect();
                await apiClient.RegisterHostAsync(hostInfo, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Host registration failed — collection will proceed normally");
            }

            // 2. Register agent to obtain AgentId for heartbeats (non-fatal).
            // If the backend is unreachable at startup, registration is retried automatically
            // at the start of every subsequent cycle (see TryRegisterAgentAsync).
            await TryRegisterAgentAsync(stoppingToken);

            // 3. Run immediately on service start, then repeat on the timer
            await RunTimedCycleAsync(++cycleNumber, opts, stoppingToken);

            using var timer = new PeriodicTimer(TimeSpan.FromMinutes(Math.Max(1, opts.PollIntervalMinutes)));

            while (await timer.WaitForNextTickAsync(stoppingToken))
                await RunTimedCycleAsync(++cycleNumber, opts, stoppingToken);

            // ── Zombie guard ──────────────────────────────────────────────────
            // WaitForNextTickAsync only returns false when the timer is disposed
            // (i.e. stoppingToken fired). If somehow we fall through without the
            // token being set, the service appears "Running" in SCM but is dead.
            if (!stoppingToken.IsCancellationRequested)
                logger.LogCritical(
                    "ExecuteAsync exited the main loop WITHOUT the stop token being cancelled — " +
                    "service appears running in SCM but is no longer collecting. " +
                    "Completed {Cycles} cycle(s). Review logs above for the root cause.",
                    cycleNumber);
        }
        catch (OperationCanceledException ex)
        {
            // Normal stop — SCM sent Stop or machine is shutting down
            logger.LogInformation(
                ex,
                "ExecuteAsync stopped cleanly after {Cycles} cycle(s). CancellationToken cancelled={Cancelled}",
                cycleNumber, stoppingToken.IsCancellationRequested);
        }
        catch (Exception ex)
        {
            // Unhandled crash — service will still appear "Running" in SCM until the
            // host shuts down. Log as Critical so it's visible in the Event Log.
            logger.LogCritical(ex,
                "ExecuteAsync terminated with an unhandled exception after {Cycles} cycle(s) — " +
                "service is now a zombie (SCM shows Running but no collection is occurring). " +
                "A service restart is required.",
                cycleNumber);
            throw; // re-throw so IHostedService propagates it to the host
        }
        finally
        {
            logger.LogInformation(
                "=== SQL Monitor ExecuteAsync exited — PID={Pid} TotalCycles={Cycles} ===",
                proc.Id, cycleNumber);
        }
    }

    // ── Per-cycle wrapper with timeout and duration logging ───────────────────

    private async Task RunTimedCycleAsync(int cycleNumber, SqlMonitorOptions opts, CancellationToken ct)
    {
        // Timeout = 90 % of the poll interval, floor 60 s.
        // A cycle that exceeds this is considered hung and is cancelled.
        var timeoutSec = Math.Max(60, Math.Max(1, opts.PollIntervalMinutes) * 54);

        logger.LogInformation("--- Cycle #{Cycle} starting (timeout={Timeout}s) ---",
            cycleNumber, timeoutSec);

        var sw = Stopwatch.StartNew();

        using var cycleCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cycleCts.CancelAfter(TimeSpan.FromSeconds(timeoutSec));

        try
        {
            await RunCycleAndHeartbeatAsync(cycleCts.Token);

            logger.LogInformation("--- Cycle #{Cycle} completed in {Ms}ms ---",
                cycleNumber, sw.ElapsedMilliseconds);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // The service stop token is NOT cancelled — this is the cycle-level timeout
            logger.LogCritical(
                "Cycle #{Cycle} TIMED OUT after {TimeoutSec}s on {Machine} — " +
                "a task is hung (SQL connection, API call, or PowerShell execution). " +
                "Skipping this cycle and waiting for the next tick.",
                cycleNumber, timeoutSec, Environment.MachineName);
        }
    }

    // ── Registration retry ────────────────────────────────────────────────────

    /// <summary>
    /// Attempts agent registration if not yet registered. Non-fatal — silently skips if already registered.
    /// </summary>
    private async Task TryRegisterAgentAsync(CancellationToken ct)
    {
        if (healthTracker.GetAgentId().HasValue)
            return;

        var agentId = await apiClient.RegisterAgentAsync(ct);
        if (agentId.HasValue)
            healthTracker.SetAgentId(agentId.Value);
        else
            logger.LogWarning("Agent registration failed — will retry next cycle");
    }

    // ── Collection + heartbeat ────────────────────────────────────────────────

    private async Task RunCycleAndHeartbeatAsync(CancellationToken ct)
    {
        // Retry registration here in case the backend was unreachable at startup
        logger.LogInformation("Cycle phase: agent registration check starting");
        await TryRegisterAgentAsync(ct);
        logger.LogInformation("Cycle phase: agent registration check completed");

        try
        {
            logger.LogInformation("Cycle phase: SQL collection starting");
            await collector.RunCycleAsync(ct);
            logger.LogInformation("Cycle phase: SQL collection completed");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex, "Unhandled error during collection cycle");
        }

        var id = healthTracker.GetAgentId();
        if (id.HasValue)
        {
            // Collect and ship OS service status via PowerShell/systemctl (non-fatal)
            logger.LogInformation("Cycle phase: OS services collection starting");
            var osServices = await osServicesCollector.CollectAsync(ct);
            logger.LogInformation(
                "Cycle phase: OS services collection completed — available={Available} total={Total}",
                osServices.IsAvailable, osServices.Services.Count);

            logger.LogInformation("Cycle phase: sending OS services snapshot");
            await apiClient.SendOsServicesAsync(id.Value, osServices, ct);
            logger.LogInformation("Cycle phase: OS services snapshot sent");

            // Send heartbeat with per-table check status
            logger.LogInformation("Cycle phase: sending heartbeat");
            await apiClient.SendHeartbeatAsync(id.Value, healthTracker.GetSummary(), ct);
            logger.LogInformation("Cycle phase: heartbeat sent");
        }
        else
        {
            logger.LogWarning("Cycle phase: skipping OS services and heartbeat because AgentId is not available");
        }
    }
}
