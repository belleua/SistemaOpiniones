using System;
using System.Collections.Generic;
using System.Text;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using SistemaOpiniones.ETL.Models;

namespace SistemaOpiniones.ETL.Extractors;

public class DatabaseExtractor : IExtractor
{
    private readonly string _connectionString;
    private readonly DateTime _marcaAgua;
    private readonly ILogger<DatabaseExtractor> _logger;

    public DatabaseExtractor(string connectionString,
                             DateTime marcaAgua,
                             ILogger<DatabaseExtractor> logger)
    {
        _connectionString = connectionString;
        _marcaAgua = marcaAgua;
        _logger = logger;
    }

    public async Task<IEnumerable<OpinionDto>> ExtraerAsync(CancellationToken ct = default)
    {
        _logger.LogInformation("DB: extrayendo reseñas desde {MarcaAgua}", _marcaAgua);
        var resultados = new List<OpinionDto>();

        const string sql = """
            SELECT r.resena_id, r.fecha_resena, r.producto_id,
                   p.nombre        AS nombre_producto,
                   r.clasificacion, r.puntaje, r.rating,
                   r.comentario,   r.cliente_id
            FROM   dbo.resenas r
            INNER JOIN dbo.productos p ON p.producto_id = r.producto_id
            WHERE  r.fecha_resena > @marcaAgua
            ORDER BY r.fecha_resena
        """;

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@marcaAgua", _marcaAgua);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            resultados.Add(new OpinionDto
            {
                OpinionIdSrc = reader["resena_id"].ToString()!,
                Fecha = Convert.ToDateTime(reader["fecha_resena"]),
                NombreFuente = "Base de Datos",
                ProductoIdSrc = reader["producto_id"].ToString()!,
                NombreProducto = reader["nombre_producto"].ToString()!,
                Clasificacion = reader["clasificacion"].ToString()!.ToUpper(),
                PuntajeSatisfaccion = reader["puntaje"] == DBNull.Value
                                        ? null
                                        : Convert.ToDecimal(reader["puntaje"]),
                Rating = reader["rating"] == DBNull.Value
                                        ? null
                                        : Convert.ToInt16(reader["rating"]),
                ComentarioTexto = reader["comentario"].ToString(),
                ClienteIdSrc = reader["cliente_id"].ToString(),
                FuenteOrigen = "DB"
            });
        }

        _logger.LogInformation("DB: {Total} registros extraídos", resultados.Count);
        return resultados;
    }
}