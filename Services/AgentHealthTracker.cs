using System.Collections.Concurrent;
using System.Diagnostics;

namespace SingularData.Proactive.SqlMonitor.Service.Services;

/// <summary>
/// Singleton that tracks per-table collection health across cycles.
/// Follows Datadog-style check escalation:
///   success         → Ok   (consecutive errors reset to 0)
///   1st failure     → Warning
///   3rd+ failure    → Error
/// </summary>
public sealed class AgentHealthTracker
{
    private readonly ConcurrentDictionary<string, CheckStatus> _checks = new(StringComparer.OrdinalIgnoreCase);
    private readonly DateTime _startedAtUtc = DateTime.UtcNow;
    private Guid? _agentId;

    // -------------------------------------------------------------------------
    // AgentId — set once after successful agent registration
    // -------------------------------------------------------------------------

    public void SetAgentId(Guid id) => _agentId = id;
    public Guid? GetAgentId() => _agentId;

    // -------------------------------------------------------------------------
    // Record a collection result for one table
    // -------------------------------------------------------------------------

    public void RecordResult(string instanceName, string tableName, bool success, int rowsCollected, long durationMs, string? errorMessage = null)
    {
        var key   = $"{instanceName}/{tableName}";
        var check = _checks.GetOrAdd(key, _ => new CheckStatus(instanceName, tableName));
        check.Record(success, rowsCollected, durationMs, errorMessage);
    }

    // -------------------------------------------------------------------------
    // Summary — computed from current check states
    // -------------------------------------------------------------------------

    public AgentHealthSummary GetSummary()
    {
        var checks = _checks.Values.ToList();

        var checksOk      = checks.Count(c => c.State == CheckState.Ok);
        var checksWarning = checks.Count(c => c.State == CheckState.Warning);
        var checksError   = checks.Count(c => c.State == CheckState.Error);

        // Overall status: Error only if every check that has run is failing;
        // Degraded if some are failing but at least one is Ok.
        string overallStatus;
        if (checksError > 0 && checksOk == 0 && checksWarning == 0)
            overallStatus = "Error";
        else if (checksError > 0 || checksWarning > 0)
            overallStatus = "Degraded";
        else
            overallStatus = "Online";

        var failCount = checksError + checksWarning;
        var statusMessage = overallStatus switch
        {
            "Online"   => $"Online — {checksOk} check(s) passing",
            "Degraded" => $"Degraded — {failCount} check(s) failing",
            _          => $"Error — {checksError} check(s) in error"
        };

        return new AgentHealthSummary
        {
            OverallStatus  = overallStatus,
            StatusMessage  = statusMessage,
            UptimeSeconds  = (long)(DateTime.UtcNow - _startedAtUtc).TotalSeconds,
            ChecksTotal    = checks.Count,
            ChecksOk       = checksOk,
            ChecksWarning  = checksWarning,
            ChecksError    = checksError,
            Checks         = checks.Select(c => c.ToDto()).ToList()
        };
    }
}

// ---------------------------------------------------------------------------------
// Check state per table — NOT thread-safe on its own; all mutations go through lock
// ---------------------------------------------------------------------------------

public enum CheckState { Unknown, Ok, Warning, Error }

public sealed class CheckStatus
{
    private readonly Lock _lock = new();

    public string InstanceName { get; }
    public string TableName    { get; }
    public CheckState State { get; private set; } = CheckState.Unknown;
    public DateTime? LastRunAtUtc { get; private set; }
    public DateTime? LastSuccessAtUtc { get; private set; }
    public DateTime? LastErrorAtUtc { get; private set; }
    public string? LastErrorMessage { get; private set; }
    public int ConsecutiveErrors { get; private set; }
    public int RowsCollectedLast { get; private set; }
    public long DurationMs { get; private set; }

    public CheckStatus(string instanceName, string tableName)
    {
        InstanceName = instanceName;
        TableName    = tableName;
    }

    public void Record(bool success, int rowsCollected, long durationMs, string? errorMessage)
    {
        lock (_lock)
        {
            LastRunAtUtc = DateTime.UtcNow;
            DurationMs   = durationMs;

            if (success)
            {
                State              = CheckState.Ok;
                ConsecutiveErrors  = 0;
                RowsCollectedLast  = rowsCollected;
                LastSuccessAtUtc   = DateTime.UtcNow;
            }
            else
            {
                ConsecutiveErrors++;
                RowsCollectedLast  = 0;
                LastErrorAtUtc     = DateTime.UtcNow;
                LastErrorMessage   = errorMessage;

                // Warning on 1st failure, Error on 3rd+
                State = ConsecutiveErrors >= 3 ? CheckState.Error : CheckState.Warning;
            }
        }
    }

    public CheckStatusDto ToDto()
    {
        lock (_lock)
        {
            return new CheckStatusDto
            {
                InstanceName      = InstanceName,
                TableName         = TableName,
                State             = State.ToString(),
                LastRunAtUtc      = LastRunAtUtc,
                LastSuccessAtUtc  = LastSuccessAtUtc,
                LastErrorAtUtc    = LastErrorAtUtc,
                LastErrorMessage  = LastErrorMessage,
                ConsecutiveErrors = ConsecutiveErrors,
                RowsCollectedLast = RowsCollectedLast,
                DurationMs        = DurationMs
            };
        }
    }
}

// ---------------------------------------------------------------------------------
// Summary DTO — passed to ProactiveApiClient for serialization
// ---------------------------------------------------------------------------------

public sealed class CheckStatusDto
{
    public string InstanceName    { get; set; } = string.Empty;
    public string TableName       { get; set; } = string.Empty;
    public string State           { get; set; } = "Unknown";
    public DateTime? LastRunAtUtc { get; set; }
    public DateTime? LastSuccessAtUtc { get; set; }
    public DateTime? LastErrorAtUtc   { get; set; }
    public string? LastErrorMessage   { get; set; }
    public int ConsecutiveErrors  { get; set; }
    public int RowsCollectedLast  { get; set; }
    public long DurationMs        { get; set; }
}

public sealed class AgentHealthSummary
{
    public string OverallStatus { get; set; } = "Online";
    public string StatusMessage { get; set; } = string.Empty;
    public long UptimeSeconds { get; set; }
    public int ChecksTotal { get; set; }
    public int ChecksOk { get; set; }
    public int ChecksWarning { get; set; }
    public int ChecksError { get; set; }
    public List<CheckStatusDto> Checks { get; set; } = [];
}
