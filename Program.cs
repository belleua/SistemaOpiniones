using SistemaOpiniones.ETL;
using SistemaOpiniones.ETL;
using SistemaOpiniones.ETL.Extractors;
using SistemaOpiniones.ETL.Services;

internal class Program
{
    private static void Main(string[] args)
    {
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

        
        builder.Services.AddSingleton(sp =>
            new DataLoader(
                cfg["DbConnectionString"]!,
                sp.GetRequiredService<ILogger<DataLoader>>()));

        builder.Services.AddHostedService<Worker>();

        var host = builder.Build();
        host.Run();
    }
}