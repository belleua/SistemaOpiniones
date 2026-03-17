using Microsoft.Win32;
using Serilog;
using SistemaOpiniones.ETL;
using SistemaOpiniones.ETL.Extractors;
using static System.Runtime.InteropServices.JavaScript.JSType;

var builder = Host.CreateApplicationBuilder(args);
var cfg = builder.Configuration.GetSection("ETL");

builder.Services.AddSingleton<IExtractor>(sp =>
    new CsvExtractor(
        cfg["CsvSurveys"]!,
        cfg["CsvWebReviews"]!,
        cfg["CsvSocialComments"]!,
        cfg["CsvClients"]!,
        cfg["CsvProducts"]!,
        cfg["CsvFuentes"]!,
        sp.GetRequiredService<ILogger<CsvExtractor>>()));

builder.Services.AddSingleton<IExtractor>(sp =>
    new DatabaseExtractor(
        cfg["DbConnectionString"]!,
        DateTime.Parse(cfg["MarcaAgua"]!),
        sp.GetRequiredService<ILogger<DatabaseExtractor>>()));

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
