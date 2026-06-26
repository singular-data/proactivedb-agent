namespace SingularData.Proactive.SqlMonitor.Service.Services;

/// <summary>
/// Utilitários de texto partilhados por vários serviços.
/// </summary>
internal static class StringHelpers
{
    /// <summary>
    /// Trunca <paramref name="s"/> para no máximo <paramref name="max"/> caracteres,
    /// acrescentando "…" quando o valor é cortado.
    /// Devolve <see cref="string.Empty"/> quando <paramref name="s"/> é null.
    /// </summary>
    internal static string Truncate(string? s, int max) =>
        s is null ? string.Empty : s.Length <= max ? s : s[..max] + "…";
}
