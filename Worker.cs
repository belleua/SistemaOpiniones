using SistemaOpiniones.ETL.Extractors;

namespace SistemaOpiniones.ETL;

public class Worker : BackgroundService
{
    private readonly IEnumerable<IExtractor> _extractors;
    private readonly ILogger<Worker> _logger;

    public Worker(IEnumerable<IExtractor> extractors, ILogger<Worker> logger)
    {
        _extractors = extractors;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _logger.LogInformation("ETL iniciado: {Hora}", DateTime.Now);
        var sw = System.Diagnostics.Stopwatch.StartNew();

        // Ejecutar ambos extractores en paralelo
        var tareas = _extractors.Select(e => e.ExtraerAsync(ct));
        var resultados = await Task.WhenAll(tareas);
        var todos = resultados.SelectMany(r => r).ToList();

        _logger.LogInformation("Extracción completa: {N} opiniones en {Ms}ms",
                               todos.Count, sw.ElapsedMilliseconds);

        // Aquí los datos quedan listos en memoria (staging)
        // La fase T y L viene en la siguiente práctica
    }
}