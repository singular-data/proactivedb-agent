using System.Text;
using Microsoft.Extensions.Logging;

namespace SingularData.Proactive.SqlMonitor.Service.Logging;

/// <summary>
/// Writes log entries to daily rolling files: logs/Collector_yyyy_MM_dd.log
/// Thread-safe. No external dependencies.
/// Deletes files older than <see cref="RetentionDays"/> on startup.
/// </summary>
public sealed class FileLoggerProvider : ILoggerProvider
{
    private const int RetentionDays = 30;

    private readonly string _logDirectory;
    private readonly object _lock = new();
    private StreamWriter? _writer;
    private DateOnly _currentDate;

    public FileLoggerProvider(string logDirectory)
    {
        _logDirectory = logDirectory;
        Directory.CreateDirectory(logDirectory);
        PurgeOldLogs();
    }

    private void PurgeOldLogs()
    {
        try
        {
            var cutoff = DateTime.UtcNow.AddDays(-RetentionDays);
            foreach (var file in Directory.EnumerateFiles(_logDirectory, "Collector_*.log"))
            {
                if (File.GetLastWriteTimeUtc(file) < cutoff)
                    File.Delete(file);
            }
        }
        catch { /* non-fatal — log rotation failure must not prevent the service from starting */ }
    }

    public ILogger CreateLogger(string categoryName) =>
        new FileLogger(categoryName, this);

    internal void Write(string line)
    {
        lock (_lock)
        {
            var today = DateOnly.FromDateTime(DateTime.Now);

            if (_writer is null || today != _currentDate)
            {
                _writer?.Dispose();
                _currentDate = today;
                var path = Path.Combine(_logDirectory, $"Collector_{today:yyyy_MM_dd}.log");
                _writer = new StreamWriter(path, append: true, Encoding.UTF8) { AutoFlush = true };
            }

            _writer.WriteLine(line);
        }
    }

    public void Dispose()
    {
        lock (_lock)
            _writer?.Dispose();
    }
}

internal sealed class FileLogger : ILogger
{
    private readonly string _category;
    private readonly FileLoggerProvider _provider;

    // Keep only the short class name to avoid very long lines
    private static string Shorten(string category)
    {
        var dot = category.LastIndexOf('.');
        return dot >= 0 ? category[(dot + 1)..] : category;
    }

    public FileLogger(string category, FileLoggerProvider provider)
    {
        _category = Shorten(category);
        _provider = provider;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;

        var level = logLevel switch
        {
            LogLevel.Information  => "INF",
            LogLevel.Warning      => "WRN",
            LogLevel.Error        => "ERR",
            LogLevel.Critical     => "CRT",
            _                     => logLevel.ToString()[..3].ToUpper()
        };

        var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} [{level}] {_category}: {formatter(state, exception)}";

        if (exception is not null)
            line += Environment.NewLine + exception;

        _provider.Write(line);
    }
}
