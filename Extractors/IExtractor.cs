using System;
using System.Collections.Generic;
using System.Text;
using SistemaOpiniones.ETL.Models;

namespace SistemaOpiniones.ETL.Extractors;

public interface IExtractor
{
    Task<IEnumerable<OpinionDto>> ExtraerAsync(CancellationToken ct = default);
}

