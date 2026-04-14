using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using SistemaOpiniones.ETL.Models;

namespace SistemaOpiniones.ETL.Services;

public class DataLoader
{
    private readonly string _connectionString;
    private readonly ILogger<DataLoader> _logger;

    public DataLoader(string connectionString, ILogger<DataLoader> logger)
    {
        _connectionString = connectionString;
        _logger = logger;
    }

    public async Task CargarAsync(IEnumerable<OpinionDto> opiniones,
                                  string jobId,
                                  CancellationToken ct = default)
    {
        var lista = opiniones.ToList();
        _logger.LogInformation("DataLoader: iniciando carga de {N} registros", lista.Count);

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        var jobSk = await RegistrarInicioJobAsync(conn, jobId, lista.Count, ct);

        int insertados = 0, rechazados = 0;

        foreach (var op in lista)
        {
            try
            {
                int tiempoSk = await UpsertDimTiempoAsync(conn, op.Fecha, ct);
                int fuenteSk = await UpsertDimFuenteAsync(conn, op.NombreFuente, op.FuenteOrigen, ct);
                int productoSk = await UpsertDimProductoAsync(conn, op.ProductoIdSrc, op.NombreProducto, ct);
                int? clienteSk = op.ClienteIdSrc != null
                                        ? await UpsertDimClienteAsync(conn, op.ClienteIdSrc, ct)
                                        : null;
                int clasificacionSk = ResolverClasificacion(op.Clasificacion);

                await InsertarFactOpinionAsync(conn, op, tiempoSk, fuenteSk,
                                               productoSk, clienteSk,
                                               clasificacionSk, jobId, ct);
                insertados++;
            }
            catch (Exception ex)
            {
                rechazados++;
                _logger.LogWarning("Registro rechazado {Id}: {Msg}", op.OpinionIdSrc, ex.Message);
            }
        }

        await RegistrarFinJobAsync(conn, jobSk, insertados, rechazados, ct);

        _logger.LogInformation("DataLoader: Insertados={I} Rechazados={R}", insertados, rechazados);
    }

    // ── dim_tiempo ────────────────────────────────────────────────────────────
    private static async Task<int> UpsertDimTiempoAsync(
        SqlConnection conn, DateTime fecha, CancellationToken ct)
    {
        const string sql = """
            MERGE dw_opiniones.dim_tiempo AS target
            USING (SELECT @fecha AS fecha) AS source ON target.fecha = source.fecha
            WHEN NOT MATCHED THEN
                INSERT (fecha, anio, trimestre, mes, nombre_mes,
                        semana_anio, dia_semana, es_fin_semana)
                VALUES (@fecha, @anio, @trimestre, @mes, @nombreMes,
                        @semanaAnio, @diaSemana, @esFinde);
            SELECT tiempo_sk FROM dw_opiniones.dim_tiempo WHERE fecha = @fecha;
        """;

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@fecha", fecha.Date);
        cmd.Parameters.AddWithValue("@anio", fecha.Year);
        cmd.Parameters.AddWithValue("@trimestre", (fecha.Month - 1) / 3 + 1);
        cmd.Parameters.AddWithValue("@mes", fecha.Month);
        cmd.Parameters.AddWithValue("@nombreMes", fecha.ToString("MMMM"));
        cmd.Parameters.AddWithValue("@semanaAnio",
            System.Globalization.CultureInfo.CurrentCulture.Calendar
                .GetWeekOfYear(fecha,
                    System.Globalization.CalendarWeekRule.FirstDay,
                    DayOfWeek.Monday));
        cmd.Parameters.AddWithValue("@diaSemana", fecha.DayOfWeek.ToString());
        cmd.Parameters.AddWithValue("@esFinde",
            fecha.DayOfWeek == DayOfWeek.Saturday ||
            fecha.DayOfWeek == DayOfWeek.Sunday ? 1 : 0);

        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
    }

    // ── dim_fuente ────────────────────────────────────────────────────────────
    private static async Task<int> UpsertDimFuenteAsync(
        SqlConnection conn, string nombreFuente, string tipoFuente, CancellationToken ct)
    {
        const string sql = """
            MERGE dw_opiniones.dim_fuente AS target
            USING (SELECT @nombre AS nombre_fuente) AS source
                  ON target.nombre_fuente = source.nombre_fuente
            WHEN NOT MATCHED THEN
                INSERT (nombre_fuente, tipo_fuente, es_digital)
                VALUES (@nombre, @tipo, 1);
            SELECT fuente_sk FROM dw_opiniones.dim_fuente
            WHERE nombre_fuente = @nombre;
        """;

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@nombre", nombreFuente);
        cmd.Parameters.AddWithValue("@tipo", tipoFuente);

        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
    }

    // ── dim_producto ──────────────────────────────────────────────────────────
    private static async Task<int> UpsertDimProductoAsync(
        SqlConnection conn, string productoIdSrc, string nombreProducto, CancellationToken ct)
    {
        const string sql = """
            MERGE dw_opiniones.dim_producto AS target
            USING (SELECT @idSrc AS producto_id_src) AS source
                  ON target.producto_id_src = source.producto_id_src
            WHEN NOT MATCHED THEN
                INSERT (producto_id_src, nombre_producto, id_categoria, nombre_categoria)
                VALUES (@idSrc, @nombre, 0, 'Sin categoria');
            SELECT producto_sk FROM dw_opiniones.dim_producto
            WHERE producto_id_src = @idSrc;
        """;

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@idSrc", productoIdSrc);
        cmd.Parameters.AddWithValue("@nombre", nombreProducto);

        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
    }

    // ── dim_cliente ───────────────────────────────────────────────────────────
    private static async Task<int> UpsertDimClienteAsync(
        SqlConnection conn, string clienteIdSrc, CancellationToken ct)
    {
        const string sql = """
            MERGE dw_opiniones.dim_cliente AS target
            USING (SELECT @idSrc AS cliente_id_src) AS source
                  ON target.cliente_id_src = source.cliente_id_src
            WHEN NOT MATCHED THEN
                INSERT (cliente_id_src) VALUES (@idSrc);
            SELECT cliente_sk FROM dw_opiniones.dim_cliente
            WHERE cliente_id_src = @idSrc;
        """;

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@idSrc", clienteIdSrc);

        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
    }

    // ── clasificacion (valores fijos ya insertados en BD) ─────────────────────
    private static int ResolverClasificacion(string clasificacion) =>
        clasificacion.ToUpper() switch
        {
            "POSITIVO" => 1,
            "NEGATIVO" => 2,
            _ => 3
        };

    // ── fact_opinion ──────────────────────────────────────────────────────────
    private static async Task InsertarFactOpinionAsync(
        SqlConnection conn, OpinionDto op,
        int tiempoSk, int fuenteSk, int productoSk,
        int? clienteSk, int clasificacionSk,
        string jobId, CancellationToken ct)
    {
        const string sql = """
            IF NOT EXISTS (
                SELECT 1 FROM dw_opiniones.fact_opinion
                WHERE opinion_id_src = @opinionId AND fuente_sk = @fuenteSk
            )
            INSERT INTO dw_opiniones.fact_opinion
                (tiempo_sk, fuente_sk, producto_sk, cliente_sk,
                 clasificacion_sk, puntaje_satisfaccion, rating,
                 comentario_texto, opinion_id_src, etl_job_id, etl_fuente_archivo)
            VALUES
                (@tiempoSk, @fuenteSk, @productoSk, @clienteSk,
                 @clasificacionSk, @puntaje, @rating,
                 @comentario, @opinionId, @jobId, @fuente);
        """;

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@tiempoSk", tiempoSk);
        cmd.Parameters.AddWithValue("@fuenteSk", fuenteSk);
        cmd.Parameters.AddWithValue("@productoSk", productoSk);
        cmd.Parameters.AddWithValue("@clienteSk", (object?)clienteSk ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@clasificacionSk", clasificacionSk);
        cmd.Parameters.AddWithValue("@puntaje", (object?)op.PuntajeSatisfaccion ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@rating", (object?)op.Rating ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@comentario", (object?)op.ComentarioTexto ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@opinionId", op.OpinionIdSrc);
        cmd.Parameters.AddWithValue("@jobId", jobId);
        cmd.Parameters.AddWithValue("@fuente", op.FuenteOrigen);

        await cmd.ExecuteNonQueryAsync(ct);
    }

    // ── etl_control_carga ─────────────────────────────────────────────────────
    private static async Task<long> RegistrarInicioJobAsync(
        SqlConnection conn, string jobId, int total, CancellationToken ct)
    {
        const string sql = """
            INSERT INTO dw_opiniones.etl_control_carga
                (job_nombre, estado_job, fecha_inicio, registros_procesados, version_pipeline)
            OUTPUT INSERTED.job_sk
            VALUES (@jobId, 'EN_PROCESO', SYSDATETIME(), @total, '1.0');
        """;

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@jobId", jobId);
        cmd.Parameters.AddWithValue("@total", total);

        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
    }

    private static async Task RegistrarFinJobAsync(
        SqlConnection conn, long jobSk,
        int insertados, int rechazados, CancellationToken ct)
    {
        const string sql = """
            UPDATE dw_opiniones.etl_control_carga
            SET    estado_job           = CASE WHEN @rechazados = 0
                                              THEN 'COMPLETADO'
                                              ELSE 'COMPLETADO_CON_ERRORES' END,
                   fecha_fin            = SYSDATETIME(),
                   registros_insertados = @insertados,
                   registros_rechazados = @rechazados
            WHERE  job_sk = @jobSk;
        """;

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@jobSk", jobSk);
        cmd.Parameters.AddWithValue("@insertados", insertados);
        cmd.Parameters.AddWithValue("@rechazados", rechazados);

        await cmd.ExecuteNonQueryAsync(ct);
    }
}