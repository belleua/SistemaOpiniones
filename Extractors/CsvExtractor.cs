using System;
using System.Collections.Generic;
using System.Text;

using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Logging;
using SistemaOpiniones.ETL.Models;
using System.Globalization;

namespace SistemaOpiniones.ETL.Extractors;

public class CsvExtractor : IExtractor
{
    private readonly string _rutaSurveys;
    private readonly string _rutaWebReviews;
    private readonly string _rutaSocialComments;
    private readonly string _rutaClients;
    private readonly string _rutaProducts;
    private readonly string _rutaFuentes;
    private readonly ILogger<CsvExtractor> _logger;

    // Diccionarios de referencia cargados al inicio
    private Dictionary<string, string> _clientes = new();
    private Dictionary<string, string> _productos = new();
    private Dictionary<string, string> _fuentes = new();

    public CsvExtractor(string rutaSurveys,
                        string rutaWebReviews,
                        string rutaSocialComments,
                        string rutaClients,
                        string rutaProducts,
                        string rutaFuentes,
                        ILogger<CsvExtractor> logger)
    {
        _rutaSurveys = rutaSurveys;
        _rutaWebReviews = rutaWebReviews;
        _rutaSocialComments = rutaSocialComments;
        _rutaClients = rutaClients;
        _rutaProducts = rutaProducts;
        _rutaFuentes = rutaFuentes;
        _logger = logger;
    }

    public async Task<IEnumerable<OpinionDto>> ExtraerAsync(CancellationToken ct = default)
    {
        // Primero cargar los datos de referencia
        await CargarClientesAsync(ct);
        await CargarProductosAsync(ct);
        await CargarFuentesAsync(ct);

        var resultados = new List<OpinionDto>();
        resultados.AddRange(await LeerSurveysAsync(ct));
        resultados.AddRange(await LeerWebReviewsAsync(ct));
        resultados.AddRange(await LeerSocialCommentsAsync(ct));

        _logger.LogInformation("CSV total: {Total} registros extraídos", resultados.Count);
        return resultados;
    }

    // ── Cargar datos de referencia ───────────────────────────────────────────

    private async Task CargarClientesAsync(CancellationToken ct)
    {
        if (!File.Exists(_rutaClients)) return;
        var contenido = await File.ReadAllTextAsync(_rutaClients, ct);
        using var reader = new StringReader(contenido);
        using var csv = new CsvReader(reader, ConfigBase());
        await csv.ReadAsync(); csv.ReadHeader();
        while (await csv.ReadAsync())
        {
            var id = csv.GetField("IdCliente") ?? "";
            var nombre = csv.GetField("Nombre") ?? "";
            if (!string.IsNullOrEmpty(id))
                _clientes[id] = nombre;
        }
        _logger.LogInformation("Referencia clientes: {N}", _clientes.Count);
    }

    private async Task CargarProductosAsync(CancellationToken ct)
    {
        if (!File.Exists(_rutaProducts)) return;
        var contenido = await File.ReadAllTextAsync(_rutaProducts, ct);
        using var reader = new StringReader(contenido);
        using var csv = new CsvReader(reader, ConfigBase());
        await csv.ReadAsync(); csv.ReadHeader();
        while (await csv.ReadAsync())
        {
            var id = csv.GetField("IdProducto") ?? "";
            var nombre = csv.GetField("Nombre") ?? "";
            if (!string.IsNullOrEmpty(id))
                _productos[id] = nombre;
        }
        _logger.LogInformation("Referencia productos: {N}", _productos.Count);
    }

    private async Task CargarFuentesAsync(CancellationToken ct)
    {
        if (!File.Exists(_rutaFuentes)) return;
        var contenido = await File.ReadAllTextAsync(_rutaFuentes, ct);
        using var reader = new StringReader(contenido);
        using var csv = new CsvReader(reader, ConfigBase());
        await csv.ReadAsync(); csv.ReadHeader();
        while (await csv.ReadAsync())
        {
            var id = csv.GetField("IdFuente") ?? "";
            var tipo = csv.GetField("TipoFuente") ?? "";
            if (!string.IsNullOrEmpty(id))
                _fuentes[id] = tipo;
        }
        _logger.LogInformation("Referencia fuentes: {N}", _fuentes.Count);
    }

    // ── Leer opiniones ───────────────────────────────────────────────────────

    private async Task<List<OpinionDto>> LeerSurveysAsync(CancellationToken ct)
    {
        _logger.LogInformation("CSV: leyendo surveys desde {Ruta}", _rutaSurveys);
        var lista = new List<OpinionDto>();
        if (!File.Exists(_rutaSurveys))
        {
            _logger.LogWarning("CSV: no encontrado {Ruta}", _rutaSurveys);
            return lista;
        }

        var contenido = await File.ReadAllTextAsync(_rutaSurveys, ct);
        using var reader = new StringReader(contenido);
        using var csv = new CsvReader(reader, ConfigBase());
        await csv.ReadAsync();
        csv.ReadHeader();

        while (await csv.ReadAsync())
        {
            var idProducto = csv.GetField("IdProducto") ?? "";
            var idCliente = csv.GetField("IdCliente") ?? "";

            lista.Add(new OpinionDto
            {
                OpinionIdSrc = $"ENC-{csv.GetField("IdOpinion")}",
                Fecha = DateTime.Parse(csv.GetField("Fecha")!),
                NombreFuente = csv.GetField("Fuente") ?? "EncuestaInterna",
                ProductoIdSrc = idProducto,
                NombreProducto = _productos.GetValueOrDefault(idProducto,
                                        $"Producto_{idProducto}"),
                Clasificacion = NormalizarClasificacion(
                                        csv.GetField("Clasificación") ??
                                        csv.GetField("Clasificacion") ?? ""),
                PuntajeSatisfaccion = decimal.TryParse(
                                        csv.GetField("PuntajeSatisfacción") ??
                                        csv.GetField("PuntajeSatisfaccion"),
                                        out var p) ? p : null,
                ComentarioTexto = csv.GetField("Comentario"),
                ClienteIdSrc = idCliente,
                FuenteOrigen = "CSV"
            });
        }

        _logger.LogInformation("Surveys: {N} registros", lista.Count);
        return lista;
    }

    private async Task<List<OpinionDto>> LeerWebReviewsAsync(CancellationToken ct)
    {
        _logger.LogInformation("CSV: leyendo web_reviews desde {Ruta}", _rutaWebReviews);
        var lista = new List<OpinionDto>();
        if (!File.Exists(_rutaWebReviews))
        {
            _logger.LogWarning("CSV: no encontrado {Ruta}", _rutaWebReviews);
            return lista;
        }

        var contenido = await File.ReadAllTextAsync(_rutaWebReviews, ct);
        using var reader = new StringReader(contenido);
        using var csv = new CsvReader(reader, ConfigBase());
        await csv.ReadAsync();
        csv.ReadHeader();

        while (await csv.ReadAsync())
        {
            var idProducto = csv.GetField("IdProducto") ?? "";
            var idCliente = csv.GetField("IdCliente") ?? "";
            var rating = short.TryParse(csv.GetField("Rating"), out var r) ? r : (short)0;

            lista.Add(new OpinionDto
            {
                OpinionIdSrc = csv.GetField("IdReview") ?? "",
                Fecha = DateTime.Parse(csv.GetField("Fecha")!),
                NombreFuente = "Web Reviews",
                ProductoIdSrc = idProducto,
                NombreProducto = _productos.GetValueOrDefault(idProducto,
                                    $"Producto_{idProducto}"),
                Clasificacion = ClasificarPorRating(rating),
                Rating = rating,
                ComentarioTexto = csv.GetField("Comentario"),
                ClienteIdSrc = string.IsNullOrWhiteSpace(idCliente) ? null : idCliente,
                FuenteOrigen = "CSV"
            });
        }

        _logger.LogInformation("WebReviews: {N} registros", lista.Count);
        return lista;
    }

    private async Task<List<OpinionDto>> LeerSocialCommentsAsync(CancellationToken ct)
    {
        _logger.LogInformation("CSV: leyendo social_comments desde {Ruta}", _rutaSocialComments);
        var lista = new List<OpinionDto>();
        if (!File.Exists(_rutaSocialComments))
        {
            _logger.LogWarning("CSV: no encontrado {Ruta}", _rutaSocialComments);
            return lista;
        }

        var contenido = await File.ReadAllTextAsync(_rutaSocialComments, ct);
        using var reader = new StringReader(contenido);
        using var csv = new CsvReader(reader, ConfigBase());
        await csv.ReadAsync();
        csv.ReadHeader();

        while (await csv.ReadAsync())
        {
            var idProducto = csv.GetField("IdProducto") ?? "";
            var idCliente = csv.GetField("IdCliente") ?? "";
            var fuente = csv.GetField("Fuente") ?? "Social";

            lista.Add(new OpinionDto
            {
                OpinionIdSrc = csv.GetField("IdComment") ?? "",
                Fecha = DateTime.Parse(csv.GetField("Fecha")!),
                NombreFuente = fuente,
                ProductoIdSrc = idProducto,
                NombreProducto = _productos.GetValueOrDefault(idProducto,
                                    $"Producto_{idProducto}"),
                Clasificacion = "NEUTRO",
                ComentarioTexto = csv.GetField("Comentario"),
                ClienteIdSrc = string.IsNullOrWhiteSpace(idCliente) ? null : idCliente,
                FuenteOrigen = "CSV"
            });
        }

        _logger.LogInformation("SocialComments: {N} registros", lista.Count);
        return lista;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static CsvConfiguration ConfigBase() =>
        new(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            MissingFieldFound = null,
            HeaderValidated = null
        };

    private static string NormalizarClasificacion(string valor) =>
        valor.ToLower().Trim() switch
        {
            "positiva" or "positivo" => "POSITIVO",
            "negativa" or "negativo" => "NEGATIVO",
            _ => "NEUTRO"
        };

    private static string ClasificarPorRating(short rating) =>
        rating switch
        {
            >= 4 => "POSITIVO",
            <= 2 => "NEGATIVO",
            _ => "NEUTRO"
        };
}