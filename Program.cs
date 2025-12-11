    using System.Reflection;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Data.SqlClient;
    using ParquetExporter.Models;
    using ParquetExporter.Services;
    using Serilog;

    // Determine mode based on args: "discover" or default "export"
    var argsLower = args.Select(a => a.ToLowerInvariant()).ToArray();

    if (argsLower.Contains("discover"))
    {
        await DiscoverTablesAndKeysAsync();
        return;
    }

    var environment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Development";

    var configuration = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile(Path.Combine("config", environment, "appsettings.json"), optional: false, reloadOnChange: false)
        .AddEnvironmentVariables()
        .Build();

    Log.Logger = new LoggerConfiguration()
        .ReadFrom.Configuration(configuration)
        .Enrich.WithProperty("Environment", environment)
        .Enrich.WithProperty("Version", Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "unknown")
        .CreateLogger();

    try
    {
        Log.Information("ParquetExporter starting in EXPORT mode. Environment: {Environment}", environment);

        var appConfig = configuration.GetSection("AppConfig").Get<AppConfig>() ?? new AppConfig();

        var sqlConnString = Environment.GetEnvironmentVariable(appConfig.SqlConnectionStringEnvVar);
        var blobConnString = Environment.GetEnvironmentVariable(appConfig.BlobConnectionStringEnvVar);

        if (string.IsNullOrWhiteSpace(sqlConnString))
            throw new InvalidOperationException($"Environment variable '{appConfig.SqlConnectionStringEnvVar}' not set.");

        if (string.IsNullOrWhiteSpace(blobConnString))
            throw new InvalidOperationException($"Environment variable '{appConfig.BlobConnectionStringEnvVar}' not set.");

        if (appConfig.Tables == null || appConfig.Tables.Count == 0)
            throw new InvalidOperationException("No tables configured in AppConfig.Tables.");

        Log.Information("Tables to export: {Tables}",
            string.Join(", ", appConfig.Tables.Select(t => $"{t.Name} (Key={t.KeyColumn}, Batch={t.BatchSize})")));

        var exporter = new TableExporter(appConfig, blobConnString, Log.Logger);
        var cts = new CancellationTokenSource();

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = appConfig.MaxDegreeOfParallelism,
            CancellationToken = cts.Token
        };

        await Parallel.ForEachAsync(appConfig.Tables, parallelOptions,
            async (tableConfig, ct) =>
            {
                await RetryHelper.ExecuteWithRetryAsync(
                    () => exporter.ExportTableAsync(tableConfig, sqlConnString!, ct),
                    maxRetries: appConfig.Retry.MaxRetries,
                    baseDelay: TimeSpan.FromSeconds(appConfig.Retry.BaseDelaySeconds),
                    logger: Log.Logger,
                    operationName: $"ExportTable:{tableConfig.Name}");
            });

        Log.Information("All tables processed successfully. Exiting.");
    }
    catch (Exception ex)
    {
        Log.Fatal(ex, "Unhandled exception in ParquetExporter.");
    }
    finally
    {
        Log.CloseAndFlush();
    }

    static async Task DiscoverTablesAndKeysAsync()
    {
        Console.WriteLine("🔍 Discovering tables and primary keys...
");

        var sqlConn = Environment.GetEnvironmentVariable("SQL_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(sqlConn))
        {
            Console.WriteLine("❌ ERROR: SQL_CONNECTION_STRING environment variable is required.");
            return;
        }

        await using var conn = new SqlConnection(sqlConn);
        await conn.OpenAsync();

        var query = @"
    SELECT 
        t.TABLE_SCHEMA,
        t.TABLE_NAME,
        k.COLUMN_NAME,
        c.DATA_TYPE
    FROM INFORMATION_SCHEMA.TABLES t
    LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        ON t.TABLE_SCHEMA = tc.TABLE_SCHEMA
        AND t.TABLE_NAME = tc.TABLE_NAME
        AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
        ON tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
        AND tc.TABLE_SCHEMA = k.TABLE_SCHEMA
        AND tc.TABLE_NAME = k.TABLE_NAME
    LEFT JOIN INFORMATION_SCHEMA.COLUMNS c
        ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
        AND t.TABLE_NAME = c.TABLE_NAME
        AND k.COLUMN_NAME = c.COLUMN_NAME
    WHERE t.TABLE_TYPE = 'BASE TABLE'
    ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;";

        await using var cmd = new SqlCommand(query, conn);
        using var reader = await cmd.ExecuteReaderAsync();

        var results = new List<(string Schema, string Table, string? Key, string? DataType)>();

        while (await reader.ReadAsync())
        {
            results.Add((
                Schema: reader.GetString(0),
                Table: reader.GetString(1),
                Key: reader.IsDBNull(2) ? null : reader.GetString(2),
                DataType: reader.IsDBNull(3) ? null : reader.GetString(3)
            ));
        }

        Console.WriteLine("📄 Recommended AppConfig.Tables section:
");

        foreach (var group in results.GroupBy(r => (r.Schema, r.Table)))
        {
            var schema = group.Key.Schema;
            var table = group.Key.Table;

            var pk = group.FirstOrDefault(r => r.Key != null);

            if (pk.Key == null)
            {
                Console.WriteLine($"  ⚠ Table {schema}.{table} has NO primary key!");
                Console.WriteLine($"    → Will use fallback OFFSET-based chunking (slower on large tables)
");

                Console.WriteLine($@"  {{
        ""Name"": ""{schema}.{table}"",
        ""KeyColumn"": ""__NO_KEY__"",
        ""BatchSize"": 50000
      }},");
            }
            else
            {
                bool numeric = pk.DataType?.ToLower() switch
                {
                    "bigint" => true,
                    "int" => true,
                    "smallint" => true,
                    "tinyint" => true,
                    _ => false
                };

                if (!numeric)
                {
                    Console.WriteLine($"  ⚠ Table {schema}.{table} has NON-NUMERIC PK: {pk.Key} ({pk.DataType})");
                    Console.WriteLine($"    → Will require OFFSET paging (slower)
");
                }

                Console.WriteLine($@"  {{
        ""Name"": ""{schema}.{table}"",
        ""KeyColumn"": ""{pk.Key}"",
        ""BatchSize"": 50000
      }},");
            }
        }

        Console.WriteLine("
🎉 Done. Copy this into config/Production/appsettings.json under AppConfig.Tables.");
    }
