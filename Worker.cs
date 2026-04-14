using SistemaOpiniones.ETL.Extractors;
using SistemaOpiniones.ETL.Services;


namespace SistemaOpiniones.ETL;

public class Worker : BackgroundService
{
    private readonly IEnumerable<IExtractor> _extractors;
    private readonly DataLoader _loader;
    private readonly ILogger<Worker> _logger;

    public Worker()
    {
    }

    public Worker(IEnumerable<IExtractor> extractors,
                  DataLoader loader,
                  ILogger<Worker> logger)
    {
        _extractors = extractors;
        _loader = loader;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _logger.LogInformation("ETL iniciado: {Hora}", DateTime.Now);
        var sw = System.Diagnostics.Stopwatch.StartNew();

        // ── E: Extracción ────────────────────────────────────────────────────
        var tareas = _extractors.Select(e => e.ExtraerAsync(ct));
        var resultados = await Task.WhenAll(tareas);
        var todos = resultados.SelectMany(r => r).ToList();

        _logger.LogInformation("Extracción completa: {N} registros en {Ms}ms",
                               todos.Count, sw.ElapsedMilliseconds);

        // ── L: Carga al DW ───────────────────────────────────────────────────
        var jobId = $"ETL-{DateTime.Now:yyyyMMdd-HHmmss}";
        await _loader.CargarAsync(todos, jobId, ct);

        _logger.LogInformation("ETL finalizado en {Ms}ms total", sw.ElapsedMilliseconds);
    }
}