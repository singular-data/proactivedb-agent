namespace SingularData.Proactive.SqlMonitor.Service.Configuration;

public sealed class SqlMonitorOptions
{
    public const string SectionName = "SqlMonitor";

    /// <summary>Base URL of the ProactiveDB Backend API (e.g. http://localhost:5000).</summary>
    public string ApiBaseUrl { get; set; } = string.Empty;

    /// <summary>API key sent in the X-Api-Key header to authenticate with the Backend API.</summary>
    public string ApiKey { get; set; } = string.Empty;

    /// <summary>How often to poll each table, in minutes. Default: 5.</summary>
    public int PollIntervalMinutes { get; set; } = 5;

    /// <summary>Default maximum rows to fetch per table per cycle. Can be overridden per instance. Default: 100.</summary>
    public int MaxRowsPerTable { get; set; } = 100;

    /// <summary>Default maximum concurrent table collections per instance. Can be overridden per instance. Default: 4.</summary>
    public int MaxParallelTables { get; set; } = 4;

    /// <summary>Maximum SQL instances to collect concurrently. 0 = all instances at once. Default: 0.</summary>
    public int MaxParallelInstances { get; set; } = 0;

    /// <summary>SQL Server instances to monitor. When empty, falls back to the legacy ConnectionString + Tables fields.</summary>
    public List<SqlInstanceConfig> Instances { get; set; } = [];

    // ── Legacy flat config (backwards compatibility when Instances is empty) ───
    /// <summary>Legacy single-instance connection string. Prefer Instances[] for new deployments.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Legacy single-instance tables. Prefer Instances[] for new deployments.</summary>
    public List<TableMonitorConfig> Tables { get; set; } = [];

    /// <summary>When true, HTTP requests to the Backend API are routed through <see cref="ProxyUrl"/>.</summary>
    public bool UseProxy { get; set; } = false;

    /// <summary>Web proxy URL used when <see cref="UseProxy"/> is true (e.g. http://proxy.corp.local:8080).</summary>
    public string? ProxyUrl { get; set; }

    /// <summary>
    /// When true, the proxy authenticates using the Windows identity of the service account
    /// (Kerberos / NTLM Negotiate). Takes precedence over <see cref="ProxyUsername"/>.
    /// </summary>
    public bool ProxyUseDefaultCredentials { get; set; } = false;

    /// <summary>Optional proxy username. Leave empty to use default or anonymous credentials.</summary>
    public string? ProxyUsername { get; set; }

    /// <summary>Optional proxy password. Used together with <see cref="ProxyUsername"/>.</summary>
    public string? ProxyPassword { get; set; }

    /// <summary>Optional proxy domain for NTLM authentication (e.g. "CORP").</summary>
    public string? ProxyDomain { get; set; }
}

public sealed class SqlInstanceConfig
{
    /// <summary>Logical name for this SQL Server instance — used as a resource tag in metrics and health checks.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>SQL Server connection string for this instance.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Tables to monitor for this instance.</summary>
    public List<TableMonitorConfig> Tables { get; set; } = [];

    /// <summary>Per-instance override for MaxRowsPerTable. Falls back to the root option when null.</summary>
    public int? MaxRowsPerTable { get; set; }

    /// <summary>Per-instance override for MaxParallelTables. Falls back to the root option when null.</summary>
    public int? MaxParallelTables { get; set; }
}

public sealed class TableMonitorConfig
{
    /// <summary>Fully qualified table name, e.g. "dbo.MyTable".</summary>
    public string TableName { get; set; } = string.Empty;

    /// <summary>
    /// Comma-separated list of columns to extract and send to the API.
    /// When omitted or empty, all columns are selected except the internal
    /// monitoring columns (sent_monitoring, sent_monitoring_desc).
    /// </summary>
    public string? Columns { get; set; }

    /// <summary>Primary key (or unique) column used to identify rows for the UPDATE after sending. Defaults to "id".</summary>
    public string KeyColumn { get; set; } = "id";
}
