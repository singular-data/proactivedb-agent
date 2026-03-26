using System.Diagnostics;
using System.Text.Json;
using System.Text;
using System.Text.RegularExpressions; // Regex — not in this project's global usings
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SingularData.Proactive.SqlMonitor.Service.Configuration;

namespace SingularData.Proactive.SqlMonitor.Service.Services;

/// <summary>
/// Collects SQL-related OS service status without requiring a SQL Server connection.
/// Uses PowerShell + WMI on Windows, systemctl on Linux.
/// Permissions: local "Performance Monitor Users" group (Windows) or no special rights (Linux).
/// </summary>
public sealed partial class OsServicesCollector(
    ILogger<OsServicesCollector> logger,
    IOptions<SqlMonitorOptions> options)
{
    private readonly SqlConnectionTarget _target =
        SqlConnectionTarget.Parse(options.Value.ConnectionString);

    public async Task<OsServicesSnapshot> CollectAsync(CancellationToken ct)
    {
        try
        {
            var snapshot = OperatingSystem.IsWindows()
                ? await CollectWindowsAsync(ct)
                : await CollectLinuxAsync(ct);

            snapshot.ConfiguredInstanceStatus =
                ResolveConfiguredInstanceStatus(_target, snapshot);

            // Mark the service that matches the configured connection string target.
            if (_target.IsLocalhost)
            {
                if (_target.Port.HasValue)
                {
                    // Explicit port in connection string — match by the port the service is listening on.
                    // Falls back to name matching for stopped services (no port detectable yet).
                    var expectedName = _target.ExpectedServiceName;
                    foreach (var svc in snapshot.Services)
                        svc.IsMainInstance = svc.Port == _target.Port
                            || (svc.Port is null && svc.Name.Equals(expectedName, StringComparison.OrdinalIgnoreCase));
                }
                else
                {
                    // No explicit port — match by expected service name (MSSQLSERVER or MSSQL$NAME).
                    var expectedName = _target.ExpectedServiceName;
                    foreach (var svc in snapshot.Services)
                        svc.IsMainInstance = svc.Name.Equals(expectedName, StringComparison.OrdinalIgnoreCase);
                }
            }

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "OS services collected — platform={Platform} total={Total} running={Running} configured={Configured}",
                    snapshot.OsPlatform,
                    snapshot.Services.Count,
                    snapshot.Services.Count(s => s.IsRunning),
                    snapshot.ConfiguredInstanceStatus?.ToString() ?? "n/a (remote)");

            return snapshot;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "OS services collection failed");
            return new OsServicesSnapshot
            {
                IsAvailable               = false,
                OsPlatform                = OperatingSystem.IsWindows() ? "Windows" : "Linux",
                ErrorMessage              = $"{ex.GetType().Name}: {ex.Message}",
                ConfiguredInstanceStatus  = null
            };
        }
    }

    /// <summary>
    /// Determines the status of the specific SQL Server instance the agent is configured to monitor.
    /// Only meaningful when the target is the local machine — WMI service data is local-only.
    /// Returns <c>null</c> for remote targets (query failure emails cover that case).
    /// </summary>
    private static SqlServerStatus? ResolveConfiguredInstanceStatus(
        SqlConnectionTarget target, OsServicesSnapshot snapshot)
    {
        if (!target.IsLocalhost)
            return null;

        return GetLocalInstanceStatus(target, snapshot);
    }

    /// <summary>
    /// Checks the WMI service list (already collected) for the specific engine service,
    /// then looks for stopped Auto-start companions of the same instance to detect degradation.
    /// Only valid when the target host is the local machine.
    /// </summary>
    private static SqlServerStatus GetLocalInstanceStatus(
        SqlConnectionTarget target, OsServicesSnapshot snapshot)
    {
        var svcName   = target.ExpectedServiceName;
        var engineSvc = snapshot.Services.FirstOrDefault(s =>
            s.Name.Equals(svcName, StringComparison.OrdinalIgnoreCase));

        if (engineSvc is null || !engineSvc.IsRunning)
            return SqlServerStatus.Offline;

        bool degraded;

        if (target.InstanceName is not null &&
            !target.InstanceName.Equals("MSSQLSERVER", StringComparison.OrdinalIgnoreCase))
        {
            // Named instance: companion services share the "$INSTANCENAME" suffix
            // e.g. SQLAgent$S2K191, MSSQLFDLauncher$S2K191
            var suffix = $"${target.InstanceName}";
            degraded = snapshot.Services.Any(s =>
                s.Name.EndsWith(suffix, StringComparison.OrdinalIgnoreCase) &&
                !s.IsSqlEngineService &&
                s.StartMode?.Equals("Auto", StringComparison.OrdinalIgnoreCase) == true &&
                !s.IsRunning);
        }
        else
        {
            // Default instance: companions have no "$" in their name
            // e.g. SQLSERVERAGENT, MSSQLFDLauncher, SQLWriter
            degraded = snapshot.Services.Any(s =>
                !s.Name.Contains('$') &&
                !s.IsSqlEngineService &&
                s.StartMode?.Equals("Auto", StringComparison.OrdinalIgnoreCase) == true &&
                !s.IsRunning);
        }

        return degraded ? SqlServerStatus.Degraded : SqlServerStatus.Running;
    }

    // -------------------------------------------------------------------------

    private static async Task<OsServicesSnapshot> CollectWindowsAsync(CancellationToken ct)
    {
        const string wmiFilter =
            "DisplayName LIKE '%SQL%' " +
            "and DisplayName <> 'SQL Server Distributed Replay Client' " +
            "and DisplayName <> 'SQL Server Distributed Replay Controller' " +
            "and StartMode <> 'Disabled'";

        var output   = await RunPowerShellWmiAsync(wmiFilter, ct);
        var services = ParseWindowsJson(output);

        await EnrichWithPortsWindowsAsync(services, ct);

        return new OsServicesSnapshot
        {
            IsAvailable = true,
            OsPlatform  = "Windows",
            Services    = services
        };
    }

    /// <summary>
    /// Runs a Win32_Service WMI query via PowerShell and returns raw JSON output.
    /// Uses -EncodedCommand to avoid quoting issues with operators like &lt;&gt; in WMI filters.
    /// Can be reused for any Win32_Service filter (e.g. IIS services, custom app services).
    /// </summary>
    private static Task<string> RunPowerShellWmiAsync(string wmiFilter, CancellationToken ct)
    {
        var psCommand =
            $"Get-WmiObject Win32_Service -Filter \"{wmiFilter}\" " +
            "| Select-Object Name, DisplayName, State, StartMode, ProcessId " +
            "| ConvertTo-Json -Compress -Depth 1";

        var encoded = Convert.ToBase64String(Encoding.Unicode.GetBytes(psCommand));
        return RunProcessAsync("powershell.exe",
            $"-NonInteractive -NoProfile -EncodedCommand {encoded}",
            timeoutMs: 15_000, ct);
    }

    private static List<OsServiceEntry> ParseWindowsJson(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];

        var trimmed = json.TrimStart();
        // If output is not JSON (for example, formatted table text), parse as text.
        if (!trimmed.StartsWith('{') && !trimmed.StartsWith('['))
            return ParseWindowsText(json);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        // PowerShell wraps a single object as {...}, multiple as [{...}, ...]
        var elements = root.ValueKind == JsonValueKind.Array
            ? root.EnumerateArray().ToList()
            : [root];

        var result = new List<OsServiceEntry>(elements.Count);
        foreach (var el in elements)
        {
            var pid = el.TryGetProperty("ProcessId", out var pidProp) && pidProp.ValueKind == JsonValueKind.Number
                ? pidProp.GetInt32()
                : (int?)null;

            // ProcessId = 0 means not running
            if (pid == 0) pid = null;

            result.Add(new OsServiceEntry
            {
                Name        = el.TryGetProperty("Name",        out var n) ? n.GetString() ?? "" : "",
                DisplayName = el.TryGetProperty("DisplayName", out var d) ? d.GetString() ?? "" : "",
                State       = el.TryGetProperty("State",       out var s) ? s.GetString() ?? "" : "",
                StartMode   = el.TryGetProperty("StartMode",   out var m) ? m.GetString() : null,
                ProcessId   = pid
            });
        }

        return result;
    }

    private static List<OsServiceEntry> ParseWindowsText(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return [];

        var result = new List<OsServiceEntry>();

        foreach (var rawLine in text.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries))
        {
            var line = rawLine.Trim();
            if (string.IsNullOrWhiteSpace(line))
                continue;

            // Skip common headers/separators from table-like output.
            if (line.StartsWith("Name", StringComparison.OrdinalIgnoreCase)
             || line.StartsWith("-"))
                continue;

            // Expected format (columns aligned by spaces):
            // Name  DisplayName  State
            // Example:
            // MSSQL$S2K191   SQL Server (S2K191)   Running
            // Split by 2+ spaces to preserve spaces inside DisplayName.
            var parts = line.Split("  ", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (parts.Length < 3)
                continue;

            result.Add(new OsServiceEntry
            {
                Name = parts[0],
                DisplayName = parts[1],
                State = parts[2],
                StartMode = null,
                ProcessId = null
            });
        }

        return result;
    }

    // -------------------------------------------------------------------------

    private static async Task<OsServicesSnapshot> CollectLinuxAsync(CancellationToken ct)
    {
        // systemd ≥ 230 supports --output=json; fall back to text parsing
        string output;
        List<OsServiceEntry> services;

        try
        {
            output = await RunProcessAsync("systemctl",
                "list-units --type=service --all --output=json --no-pager",
                timeoutMs: 10_000, ct);
            services = ParseLinuxJson(output);
        }
        catch
        {
            // Older systemd — parse plain text
            output = await RunProcessAsync("systemctl",
                "list-units --type=service --all --no-pager",
                timeoutMs: 10_000, ct);
            services = ParseLinuxText(output);
        }

        await EnrichWithPortsLinuxAsync(services, ct);

        return new OsServicesSnapshot
        {
            IsAvailable = true,
            OsPlatform  = "Linux",
            Services    = services
        };
    }

    // -------------------------------------------------------------------------

    /// <summary>
    /// Correlates running services with their TCP listening port using Get-NetTCPConnection.
    /// Skips ephemeral ports (≥49152). Best-effort — silently ignored on failure.
    /// </summary>
    private static async Task EnrichWithPortsWindowsAsync(List<OsServiceEntry> services, CancellationToken ct)
    {
        var pids = services
            .Where(s => s.ProcessId.HasValue)
            .Select(s => s.ProcessId!.Value)
            .Distinct()
            .ToList();

        if (pids.Count == 0) return;

        var pidList    = string.Join(',', pids);
        var psCommand  =
            $"$p=@({pidList}); " +
            "Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue " +
            "| Where-Object { $_.OwningProcess -in $p } " +
            "| Select-Object LocalPort,OwningProcess " +
            "| ConvertTo-Json -Compress -Depth 1";

        var encoded = Convert.ToBase64String(Encoding.Unicode.GetBytes(psCommand));

        string output;
        try
        {
            output = await RunProcessAsync("powershell.exe",
                $"-NonInteractive -NoProfile -EncodedCommand {encoded}",
                timeoutMs: 10_000, ct);
        }
        catch { return; }

        if (string.IsNullOrWhiteSpace(output)) return;

        try
        {
            using var doc  = JsonDocument.Parse(output);
            var root       = doc.RootElement;
            var elements   = root.ValueKind == JsonValueKind.Array
                ? root.EnumerateArray().ToList()
                : [root];

            // PID → lowest non-ephemeral port (the main SQL listener)
            var pidToPort = new Dictionary<int, int>();
            foreach (var el in elements)
            {
                if (!el.TryGetProperty("OwningProcess", out var pidProp))  continue;
                if (!el.TryGetProperty("LocalPort",     out var portProp)) continue;

                var pid  = pidProp.GetInt32();
                var port = portProp.GetInt32();
                if (port >= 49152) continue;

                if (!pidToPort.TryGetValue(pid, out var existing) || port < existing)
                    pidToPort[pid] = port;
            }

            foreach (var svc in services)
                if (svc.ProcessId.HasValue && pidToPort.TryGetValue(svc.ProcessId.Value, out var port))
                    svc.Port = port;
        }
        catch { }
    }

    /// <summary>
    /// Correlates running services with their TCP listening port using <c>ss -tlnp</c>.
    /// Requires the process to run as root (or CAP_NET_ADMIN) to see other processes' sockets.
    /// Best-effort — silently ignored on failure.
    /// </summary>
    private static async Task EnrichWithPortsLinuxAsync(List<OsServiceEntry> services, CancellationToken ct)
    {
        var pids = services
            .Where(s => s.ProcessId.HasValue)
            .Select(s => s.ProcessId!.Value)
            .ToHashSet();

        if (pids.Count == 0) return;

        string output;
        try
        {
            output = await RunProcessAsync("ss", "-tlnp", timeoutMs: 5_000, ct);
        }
        catch { return; }

        // Example line:
        // LISTEN 0 128 0.0.0.0:1433 0.0.0.0:* users:(("sqlservr",pid=1234,fd=47))
        var pidToPort = new Dictionary<int, int>();
        foreach (var line in output.Split('\n'))
        {
            var pidMatch = Regex.Match(line, @"pid=(\d+)");
            if (!pidMatch.Success) continue;
            if (!int.TryParse(pidMatch.Groups[1].Value, out var pid)) continue;
            if (!pids.Contains(pid)) continue;

            // Local Address:Port is the 4th whitespace-separated token
            var portMatch = Regex.Match(line, @"[\s:](\d+)\s+\S+\s+users:");
            if (!portMatch.Success) continue;
            if (!int.TryParse(portMatch.Groups[1].Value, out var port)) continue;
            if (port >= 49152) continue;

            if (!pidToPort.TryGetValue(pid, out var existing) || port < existing)
                pidToPort[pid] = port;
        }

        foreach (var svc in services)
            if (svc.ProcessId.HasValue && pidToPort.TryGetValue(svc.ProcessId.Value, out var port))
                svc.Port = port;
    }

    private static List<OsServiceEntry> ParseLinuxJson(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];

        using var doc = JsonDocument.Parse(json);
        var result = new List<OsServiceEntry>();

        foreach (var el in doc.RootElement.EnumerateArray())
        {
            var unit = el.TryGetProperty("unit", out var u) ? u.GetString() ?? "" : "";
            if (!unit.Contains("sql", StringComparison.OrdinalIgnoreCase)
             && !unit.Contains("mssql", StringComparison.OrdinalIgnoreCase))
                continue;

            result.Add(new OsServiceEntry
            {
                Name        = unit.EndsWith(".service", StringComparison.OrdinalIgnoreCase)
                                  ? unit[..^8] : unit,
                DisplayName = el.TryGetProperty("description", out var d) ? d.GetString() ?? unit : unit,
                State       = el.TryGetProperty("active",      out var a) ? a.GetString() ?? "" : "",
                StartMode   = el.TryGetProperty("load",        out var l) ? l.GetString() : null,
                ProcessId   = null  // not included in list-units output; would need systemctl show
            });
        }

        return result;
    }

    private static List<OsServiceEntry> ParseLinuxText(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return [];

        var result = new List<OsServiceEntry>();
        foreach (var line in text.Split('\n'))
        {
            if (!line.Contains("sql", StringComparison.OrdinalIgnoreCase)
             && !line.Contains("mssql", StringComparison.OrdinalIgnoreCase))
                continue;

            // Format: "  mssql-server.service  loaded active running  Microsoft SQL Server"
            var parts = line.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 4) continue;

            var unitName = parts[0].EndsWith(".service", StringComparison.OrdinalIgnoreCase)
                ? parts[0][..^8] : parts[0];

            result.Add(new OsServiceEntry
            {
                Name        = unitName,
                DisplayName = string.Join(' ', parts.Skip(4)),
                State       = parts.Length > 2 ? parts[2] : "",   // active / inactive / failed
                StartMode   = parts.Length > 1 ? parts[1] : null, // loaded / not-found
                ProcessId   = null
            });
        }

        return result;
    }

    // -------------------------------------------------------------------------

    private static async Task<string> RunProcessAsync(
        string fileName, string arguments, int timeoutMs, CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeoutMs);

        var psi = new ProcessStartInfo(fileName, arguments)
        {
            RedirectStandardOutput = true,
            RedirectStandardError  = true,
            UseShellExecute        = false,
            CreateNoWindow         = true
        };

        using var process = new Process { StartInfo = psi };
        process.Start();

        // Read stdout and stderr concurrently to prevent deadlock:
        // if either pipe's OS buffer fills up while we're blocked on the other, the process hangs.
        var stdoutTask = process.StandardOutput.ReadToEndAsync(cts.Token);
        var stderrTask = process.StandardError.ReadToEndAsync(cts.Token);

        await Task.WhenAll(stdoutTask, stderrTask);
        await process.WaitForExitAsync(cts.Token);

        if (process.ExitCode != 0)
            throw new InvalidOperationException(
                $"{fileName} exited with code {process.ExitCode}: {stderrTask.Result.Trim()}");

        return stdoutTask.Result;
    }
}

// -------------------------------------------------------------------------
// Local snapshot types — serialized and sent to the backend API
// -------------------------------------------------------------------------

/// <summary>
/// Overall health of SQL Server on this host, derived from OS service states.
/// </summary>
public enum SqlServerStatus
{
    /// <summary>No SQL Server engine service (MSSQLSERVER / MSSQL$*) is running.</summary>
    Offline,

    /// <summary>
    /// At least one engine service is running, but one or more Auto-start services are stopped.
    /// Examples: SQLAgent crashed, MSSQLFDLauncher stopped unexpectedly.
    /// </summary>
    Degraded,

    /// <summary>Engine is running and every Auto-start service is also running.</summary>
    Running
}

public sealed class OsServicesSnapshot
{
    public DateTime CollectedAtUtc { get; set; } = DateTime.UtcNow;
    public bool    IsAvailable  { get; set; }
    public string? ErrorMessage { get; set; }
    public string  OsPlatform   { get; set; } = string.Empty;
    public List<OsServiceEntry> Services { get; set; } = [];

    /// <summary>
    /// Services that represent the SQL Server database engine only
    /// (MSSQLSERVER = default instance; MSSQL$name = named instance).
    /// Excludes Agent, Browser, Writer, SSIS, SSRS, and other supporting services.
    /// </summary>
    public IEnumerable<OsServiceEntry> SqlEngineServices =>
        Services.Where(s => s.IsSqlEngineService);

    /// <summary>
    /// True when at least one SQL Server engine service is in a running/active state.
    /// </summary>
    public bool IsAnySqlInstanceRunning =>
        SqlEngineServices.Any(s => s.IsRunning);

    /// <summary>
    /// Aggregate SQL Server health derived from OS service states.
    /// <list type="bullet">
    ///   <item><term>Offline</term><description>No engine service is running.</description></item>
    ///   <item><term>Degraded</term><description>Engine is running but at least one Auto-start service is stopped —
    ///     relies on StartMode="Auto" as the OS-level intent signal, so it covers all SQL Server versions
    ///     and named instances without hardcoding companion service names.</description></item>
    ///   <item><term>Running</term><description>Engine running and all Auto-start services are up.</description></item>
    /// </list>
    /// </summary>
    public SqlServerStatus SqlServerStatus
    {
        get
        {
            if (!IsAnySqlInstanceRunning)
                return SqlServerStatus.Offline;

            bool anyAutoStartStopped = Services.Any(s =>
                s.StartMode?.Equals("Auto", StringComparison.OrdinalIgnoreCase) == true &&
                !s.IsRunning);

            return anyAutoStartStopped ? SqlServerStatus.Degraded : SqlServerStatus.Running;
        }
    }

    /// <summary>
    /// Status of the specific SQL Server instance configured in the agent's connection string.
    /// Set by <see cref="OsServicesCollector"/> after collection.
    /// For local targets: derived from WMI service list.
    /// For remote targets: derived from a TCP connectivity probe.
    /// </summary>
    /// <summary>Null when the configured target is a remote server (not checked locally).</summary>
    public SqlServerStatus? ConfiguredInstanceStatus { get; set; }
}

public sealed class OsServiceEntry
{
    public string  Name        { get; set; } = string.Empty;
    public string  DisplayName { get; set; } = string.Empty;
    public string  State       { get; set; } = string.Empty;
    public string? StartMode   { get; set; }
    public int?    ProcessId   { get; set; }
    public int?    Port        { get; set; }

    /// <summary>
    /// True for services that are the SQL Server database engine itself.
    /// Matches MSSQLSERVER (default instance) and MSSQL$&lt;name&gt; (named instances).
    /// </summary>
    public bool IsSqlEngineService =>
        Name.Equals("MSSQLSERVER", StringComparison.OrdinalIgnoreCase) ||
        Name.StartsWith("MSSQL$",   StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// True when this service is the specific SQL Server instance targeted by the agent's connection string.
    /// Only set for local targets (WMI data is local-only); always false for remote targets.
    /// </summary>
    public bool IsMainInstance { get; set; }

    /// <summary>
    /// True when the service is configured to start automatically but is not currently running.
    /// This is the trigger condition for alerts — an Auto-start service that is down indicates
    /// an unexpected outage (crash, manual stop, failed start) rather than a planned maintenance stop.
    /// </summary>
    public bool IsAlertCondition =>
        (StartMode?.Equals("Auto",    StringComparison.OrdinalIgnoreCase) == true ||  // Windows
         StartMode?.Equals("enabled", StringComparison.OrdinalIgnoreCase) == true)    // Linux
        && !IsRunning;

    /// <summary>
    /// True when the service is in a running/active state (cross-platform).
    /// </summary>
    public bool IsRunning =>
        State.Equals("Running", StringComparison.OrdinalIgnoreCase) ||   // Windows
        State.Equals("active",  StringComparison.OrdinalIgnoreCase);     // Linux/systemd
}

// -------------------------------------------------------------------------

/// <summary>
/// Parsed SQL Server connection target from the configured connection string.
/// Handles all common Data Source formats:
/// <list type="bullet">
///   <item><c>hostname</c> — default instance, port 1433</item>
///   <item><c>hostname\INSTANCENAME</c> — named instance</item>
///   <item><c>hostname,port</c> — explicit port (any instance)</item>
///   <item><c>hostname\INSTANCENAME,port</c> — named instance with explicit port</item>
/// </list>
/// </summary>
public sealed record SqlConnectionTarget(string Host, string? InstanceName, int? Port)
{
    public static SqlConnectionTarget Parse(string connectionString)
    {
        // SqlConnectionStringBuilder normalises the connection string and exposes DataSource cleanly.
        var dataSource = new SqlConnectionStringBuilder(connectionString).DataSource.Trim();
        return ParseDataSource(dataSource);
    }

    internal static SqlConnectionTarget ParseDataSource(string dataSource)
    {
        string  host;
        string? instanceName = null;
        int?    port         = null;

        // hostname\INSTANCENAME  or  hostname\INSTANCENAME,port
        if (dataSource.Contains('\\'))
        {
            var slash = dataSource.Split('\\', 2);
            host = slash[0].Trim();
            var rest = slash[1].Trim();

            if (rest.Contains(','))
            {
                var comma = rest.Split(',', 2);
                instanceName = comma[0].Trim();
                port = int.TryParse(comma[1].Trim(), out var p) ? p : null;
            }
            else
            {
                instanceName = rest;
            }
        }
        // hostname,port  or  ip,port
        else if (dataSource.Contains(','))
        {
            var comma = dataSource.Split(',', 2);
            host = comma[0].Trim();
            port = int.TryParse(comma[1].Trim(), out var p) ? p : null;
        }
        else
        {
            host = dataSource;
        }

        return new SqlConnectionTarget(host, instanceName, port);
    }

    /// <summary>
    /// True when the Data Source refers to the local machine.
    /// WMI service queries are only valid for local targets.
    /// </summary>
    public bool IsLocalhost =>
        Host.Equals("localhost",       StringComparison.OrdinalIgnoreCase) ||
        Host.Equals("127.0.0.1")                                           ||
        Host.Equals("::1")                                                 ||
        Host.Equals("(local)",         StringComparison.OrdinalIgnoreCase) ||
        Host.Equals(".")                                                    ||
        Host.Equals(Environment.MachineName, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Expected Windows service name for the SQL Server engine of this instance.
    /// <c>MSSQLSERVER</c> for the default instance; <c>MSSQL$NAME</c> for named instances.
    /// </summary>
    public string ExpectedServiceName =>
        InstanceName is null || InstanceName.Equals("MSSQLSERVER", StringComparison.OrdinalIgnoreCase)
            ? "MSSQLSERVER"
            : $"MSSQL${InstanceName}";

}
