using System;
using System.Collections.Generic;
using System.Text;

namespace SistemaOpiniones.ETL.Models;

public class OpinionDto
{
    public string OpinionIdSrc { get; set; } = string.Empty;
    public DateTime Fecha { get; set; }
    public string NombreFuente { get; set; } = string.Empty;
    public string ProductoIdSrc { get; set; } = string.Empty;
    public string NombreProducto { get; set; } = string.Empty;
    public string Clasificacion { get; set; } = string.Empty;
    public decimal? PuntajeSatisfaccion { get; set; }
    public short? Rating { get; set; }
    public string? ComentarioTexto { get; set; }
    public string? ClienteIdSrc { get; set; }
    public string FuenteOrigen { get; set; } = string.Empty; // "CSV" o "DB"
}