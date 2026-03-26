using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SingularData.Proactive.SqlMonitor.Service;
using SingularData.Proactive.SqlMonitor.Service.Configuration;
using SingularData.Proactive.SqlMonitor.Service.Logging;
using SingularData.Proactive.SqlMonitor.Service.Services;

var logDirectory = Path.Combine(
    Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
    "ProactiveDB", "SqlMonitor", "logs");

// Pass --console (or -c) to also write logs to stdout.
// Useful when running interactively for debugging; has no effect when running as a Windows Service.
var logToConsole = args.Contains("--console", StringComparer.OrdinalIgnoreCase)
                || args.Contains("-c",        StringComparer.OrdinalIgnoreCase);

var host = Host.CreateDefaultBuilder(args)
    .UseWindowsService(o => o.ServiceName = "ProactiveDB SQL Monitor")
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.SetMinimumLevel(LogLevel.Information);
        logging.AddProvider(new FileLoggerProvider(logDirectory));
        if (logToConsole)
            logging.AddConsole();
    })
    .ConfigureServices((ctx, services) =>
    {
        // Bind configuration section
        services.Configure<SqlMonitorOptions>(
            ctx.Configuration.GetSection(SqlMonitorOptions.SectionName));

        // Typed HttpClient for Backend API
        // BaseAddress and X-Api-Key are set from config at resolution time
        services.AddHttpClient<ProactiveApiClient>((sp, client) =>
        {
            var opts = sp.GetRequiredService<IOptions<SqlMonitorOptions>>().Value;

                client.BaseAddress = new Uri(opts.ApiBaseUrl.TrimEnd('/'));
            client.DefaultRequestHeaders.Add("X-Api-Key", opts.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(60);
        })
        .ConfigurePrimaryHttpMessageHandler(sp =>
        {
            var sslLogger = sp.GetRequiredService<ILogger<ProactiveApiClient>>();
            var opts = sp.GetRequiredService<IOptions<SqlMonitorOptions>>().Value;

            var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (message, cert, chain, errors) =>
                {
                    if (errors != SslPolicyErrors.None)
                    {
                        var chainStatus = chain?.ChainStatus is { Length: > 0 }
                            ? string.Join("; ", chain.ChainStatus.Select(s => s.Status + ": " + s.StatusInformation.Trim()))
                            : "none";

                        sslLogger.LogWarning(
                            "SSL validation failed on {Machine} connecting to {Host} — " +
                            "Errors: {Errors} | Subject: {Subject} | Issuer: {Issuer} | " +
                            "Thumbprint: {Thumbprint} | ChainStatus: {ChainStatus}",
                            Environment.MachineName,
                            message.RequestUri?.Host,
                            errors,
                            cert?.Subject,
                            cert?.Issuer,
                            cert is X509Certificate2 c2 ? c2.Thumbprint : cert?.GetCertHashString(),
                            chainStatus);
                    }

                    // Keep normal validation — do NOT return true here
                    return errors == SslPolicyErrors.None;
                }
            };

            if (opts.UseProxy && !string.IsNullOrWhiteSpace(opts.ProxyUrl))
            {
                var proxy = new System.Net.WebProxy(opts.ProxyUrl);

                if (opts.ProxyUseDefaultCredentials)
                    proxy.UseDefaultCredentials = true;
                else if (!string.IsNullOrWhiteSpace(opts.ProxyUsername))
                    proxy.Credentials = new System.Net.NetworkCredential(
                        opts.ProxyUsername,
                        opts.ProxyPassword ?? string.Empty,
                        opts.ProxyDomain   ?? string.Empty);

                handler.UseProxy = true;
                handler.Proxy    = proxy;
            }
            else
            {
                handler.UseProxy = false;
            }

            return handler;
        });

        services.AddSingleton<AgentHealthTracker>();
        services.AddSingleton<SqlCollectorService>();
        services.AddSingleton<OsServicesCollector>();
        services.AddHostedService<Worker>();
    })
    .Build();

// Validate required configuration before starting — fail fast with a clear message
// rather than crashing deep inside a service on the first request.
var opts = host.Services.GetRequiredService<IOptions<SqlMonitorOptions>>().Value;

if (string.IsNullOrWhiteSpace(opts.ConnectionString))
    throw new InvalidOperationException(
        "SqlMonitor:ConnectionString is not set in appsettings.json");

if (string.IsNullOrWhiteSpace(opts.ApiBaseUrl))
    throw new InvalidOperationException(
        "SqlMonitor:ApiBaseUrl is not set in appsettings.json");

if (string.IsNullOrWhiteSpace(opts.ApiKey))
    throw new InvalidOperationException(
        "SqlMonitor:ApiKey is not set in appsettings.json");

await host.RunAsync();
