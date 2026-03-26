using System.Net.NetworkInformation;
using System.Runtime.InteropServices;
using System.Text.Json.Serialization;

namespace SingularData.Proactive.SqlMonitor.Service.Services;

/// <summary>
/// Collects host machine information to be registered with the Backend API on service startup.
/// Uses WMI on Windows and /proc files on Linux for platform-specific details.
/// </summary>
public static class HostInfoCollector
{
    public static HostRegistrationPayload Collect()
    {
        var payload = new HostRegistrationPayload
        {
            Hostname = Environment.MachineName,
            OperatingSystem = RuntimeInformation.OSDescription,
            HardwarePlatform = RuntimeInformation.ProcessArchitecture.ToString(),
            CpuLogicalProcessors = Environment.ProcessorCount,
        };

        CollectNetwork(payload);
        CollectFilesystems(payload);

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            CollectWindows(payload);
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            CollectLinux(payload);

        return payload;
    }

    private static void CollectNetwork(HostRegistrationPayload p)
    {
        try
        {
            var nic = NetworkInterface.GetAllNetworkInterfaces()
                .FirstOrDefault(n =>
                    n.OperationalStatus == OperationalStatus.Up &&
                    n.NetworkInterfaceType != NetworkInterfaceType.Loopback &&
                    n.NetworkInterfaceType != NetworkInterfaceType.Tunnel);

            if (nic is null) return;

            p.MacAddress = string.Join(":", nic.GetPhysicalAddress().GetAddressBytes()
                .Select(b => b.ToString("X2")));

            var props = nic.GetIPProperties();
            p.IpAddress = props.UnicastAddresses
                .FirstOrDefault(a => a.Address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                ?.Address.ToString();
            p.Ipv6Address = props.UnicastAddresses
                .FirstOrDefault(a => a.Address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6
                                     && !a.Address.IsIPv6LinkLocal)
                ?.Address.ToString();
        }
        catch { /* non-fatal */ }
    }

    private static void CollectFilesystems(HostRegistrationPayload p)
    {
        try
        {
            p.Filesystems = DriveInfo.GetDrives()
                .Where(d => d.IsReady)
                .Select(d => new FilesystemPayload
                {
                    VolumePath = d.VolumeLabel.Length > 0 ? d.VolumeLabel : d.RootDirectory.FullName,
                    MountPoint = d.RootDirectory.FullName,
                    TotalBytes = d.TotalSize
                })
                .ToList();
        }
        catch { /* non-fatal */ }
    }

    // Reads a single WMI property; returns null instead of throwing if missing or unconvertible.
    private static T? WmiGet<T>(System.Management.ManagementBaseObject obj, string property)
        where T : struct
    {
        try
        {
            var val = obj[property];
            if (val is null) return null;
            return (T)Convert.ChangeType(val, typeof(T));
        }
        catch { return null; }
    }

    private static string? WmiGetString(System.Management.ManagementBaseObject obj, string property)
    {
        try { return obj[property]?.ToString()?.Trim(); }
        catch { return null; }
    }

    private static void CollectWindows(HostRegistrationPayload p)
    {
        try
        {
            using var searcher = new System.Management.ManagementObjectSearcher(
                "SELECT * FROM Win32_Processor");

            foreach (System.Management.ManagementObject obj in searcher.Get())
            {
                p.CpuVendorId         = WmiGetString(obj, "Manufacturer");
                p.CpuModelName        = WmiGetString(obj, "Name");
                p.CpuCores            = WmiGet<int>(obj, "NumberOfCores");
                p.CpuLogicalProcessors = WmiGet<int>(obj, "NumberOfLogicalProcessors");
                p.CpuMhz              = WmiGet<double>(obj, "CurrentClockSpeed");
                p.CpuCacheKb          = WmiGet<int>(obj, "L3CacheSize");
                p.CpuFamily           = WmiGet<int>(obj, "Family");
                p.CpuModel            = WmiGet<int>(obj, "Model");
                // Stepping is a string field in WMI (e.g. "2")
                p.CpuStepping = int.TryParse(WmiGetString(obj, "Stepping"), out var step) ? step : null;
                break; // first CPU only
            }
        }
        catch { /* Win32_Processor query failed entirely */ }

        try
        {
            using var searcher = new System.Management.ManagementObjectSearcher(
                "SELECT * FROM Win32_ComputerSystem");
            foreach (System.Management.ManagementObject obj in searcher.Get())
            {
                p.TotalMemoryBytes = WmiGet<long>(obj, "TotalPhysicalMemory");
                break;
            }
        }
        catch { /* non-fatal */ }

        try
        {
            using var searcher = new System.Management.ManagementObjectSearcher(
                "SELECT * FROM Win32_OperatingSystem");
            foreach (System.Management.ManagementObject obj in searcher.Get())
            {
                // TotalVirtualMemorySize is in KB
                var kb = WmiGet<long>(obj, "TotalVirtualMemorySize");
                p.SwapTotalBytes = kb.HasValue ? kb.Value * 1024L : null;
                break;
            }
        }
        catch { /* non-fatal */ }

        p.KernelName    = "Windows NT";
        p.KernelRelease = Environment.OSVersion.Version.ToString();
        p.KernelVersion = Environment.OSVersion.VersionString;
        p.Machine       = RuntimeInformation.ProcessArchitecture.ToString();
    }

    private static void CollectLinux(HostRegistrationPayload p)
    {
        try
        {
            p.KernelName = "Linux";
            p.KernelRelease = File.Exists("/proc/version")
                ? File.ReadAllText("/proc/version").Split(' ').ElementAtOrDefault(2) ?? string.Empty
                : Environment.OSVersion.Version.ToString();
        }
        catch { /* non-fatal */ }

        try
        {
            if (!File.Exists("/proc/cpuinfo")) return;

            var lines = File.ReadAllLines("/proc/cpuinfo");
            string? GetField(string key) => lines
                .FirstOrDefault(l => l.StartsWith(key, StringComparison.OrdinalIgnoreCase))
                ?.Split(':', 2).ElementAtOrDefault(1)?.Trim();

            p.CpuVendorId = GetField("vendor_id");
            p.CpuModelName = GetField("model name");
            p.CpuFamily = int.TryParse(GetField("cpu family"), out var f) ? f : null;
            p.CpuModel = int.TryParse(GetField("model"), out var m) ? m : null;
            p.CpuStepping = int.TryParse(GetField("stepping"), out var s) ? s : null;
            p.CpuMhz = double.TryParse(GetField("cpu MHz"),
                System.Globalization.NumberStyles.Any,
                System.Globalization.CultureInfo.InvariantCulture, out var mhz) ? mhz : null;
            p.CpuCaches = int.TryParse(
                GetField("cache size")?.Split(' ').FirstOrDefault(), out var cache) ? cache : null;
            p.CpuCores = lines.Count(l => l.StartsWith("processor", StringComparison.OrdinalIgnoreCase));
        }
        catch { /* non-fatal */ }

        try
        {
            if (!File.Exists("/proc/meminfo")) return;

            var lines = File.ReadAllLines("/proc/meminfo");
            string? GetKb(string key) => lines
                .FirstOrDefault(l => l.StartsWith(key, StringComparison.OrdinalIgnoreCase))
                ?.Split(':')[1].Trim().Split(' ')[0];

            if (long.TryParse(GetKb("MemTotal"), out var memKb))
                p.TotalMemoryBytes = memKb * 1024L;
            if (long.TryParse(GetKb("SwapTotal"), out var swapKb))
                p.SwapTotalBytes = swapKb * 1024L;
        }
        catch { /* non-fatal */ }
    }
}

/// <summary>
/// Mirror DTO matching the backend's CreateHostRequest JSON shape.
/// </summary>
public sealed class HostRegistrationPayload
{
    [JsonPropertyName("hostname")]
    public string Hostname { get; set; } = string.Empty;

    [JsonPropertyName("ipAddress")]
    public string? IpAddress { get; set; }

    [JsonPropertyName("ipv6Address")]
    public string? Ipv6Address { get; set; }

    [JsonPropertyName("macAddress")]
    public string? MacAddress { get; set; }

    [JsonPropertyName("operatingSystem")]
    public string? OperatingSystem { get; set; }

    [JsonPropertyName("kernelName")]
    public string? KernelName { get; set; }

    [JsonPropertyName("kernelRelease")]
    public string? KernelRelease { get; set; }

    [JsonPropertyName("kernelVersion")]
    public string? KernelVersion { get; set; }

    [JsonPropertyName("machine")]
    public string? Machine { get; set; }

    [JsonPropertyName("hardwarePlatform")]
    public string? HardwarePlatform { get; set; }

    [JsonPropertyName("cpuVendorId")]
    public string? CpuVendorId { get; set; }

    [JsonPropertyName("cpuModelName")]
    public string? CpuModelName { get; set; }

    [JsonPropertyName("cpuCores")]
    public int? CpuCores { get; set; }

    [JsonPropertyName("cpuLogicalProcessors")]
    public int? CpuLogicalProcessors { get; set; }

    [JsonPropertyName("cpuMhz")]
    public double? CpuMhz { get; set; }

    [JsonPropertyName("cpuCacheKb")]
    public int? CpuCacheKb { get; set; }

    // Internal field used during Linux collection, mapped to cpuCacheKb before serialization
    [JsonIgnore]
    public int? CpuCaches
    {
        set => CpuCacheKb = value;
        get => CpuCacheKb;
    }

    [JsonPropertyName("cpuFamily")]
    public int? CpuFamily { get; set; }

    [JsonPropertyName("cpuModel")]
    public int? CpuModel { get; set; }

    [JsonPropertyName("cpuStepping")]
    public int? CpuStepping { get; set; }

    [JsonPropertyName("totalMemoryBytes")]
    public long? TotalMemoryBytes { get; set; }

    [JsonPropertyName("swapTotalBytes")]
    public long? SwapTotalBytes { get; set; }

    [JsonPropertyName("filesystems")]
    public List<FilesystemPayload> Filesystems { get; set; } = [];
}

public sealed class FilesystemPayload
{
    [JsonPropertyName("volumePath")]
    public string VolumePath { get; set; } = string.Empty;

    [JsonPropertyName("mountPoint")]
    public string? MountPoint { get; set; }

    [JsonPropertyName("totalBytes")]
    public long? TotalBytes { get; set; }
}
