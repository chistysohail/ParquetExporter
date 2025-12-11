namespace ParquetExporter.Models;

public class RetrySettings
{
    public int MaxRetries { get; set; } = 5;
    public int BaseDelaySeconds { get; set; } = 5;
}

public class TableConfig
{
    public string Name { get; set; } = string.Empty;
    public string KeyColumn { get; set; } = "Id"; // __NO_KEY__ for offset-based fallback
    public int BatchSize { get; set; } = 50000;
}

public class AppConfig
{
    public string SqlConnectionStringEnvVar { get; set; } = "SQL_CONNECTION_STRING";
    public string BlobConnectionStringEnvVar { get; set; } = "BLOB_CONNECTION_STRING";
    public string BlobContainerName { get; set; } = "parquet-exports";
    public string OutputFolder { get; set; } = "/tmp/parquet-out";
    public int MaxDegreeOfParallelism { get; set; } = 4;
    public RetrySettings Retry { get; set; } = new();
    public List<TableConfig> Tables { get; set; } = new();
}
