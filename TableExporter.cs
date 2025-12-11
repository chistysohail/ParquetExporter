using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Azure.Storage.Blobs;
using Parquet;
using Parquet.Data;
using Serilog;
using ParquetExporter.Models;

namespace ParquetExporter.Services;

internal record ManifestEntry(
    string TableName,
    int PartNumber,
    long FromKey,
    long ToKey,
    long RowCount,
    string BlobPath,
    string Status);

internal record FailedChunkInfo(
    string TableName,
    int PartNumber,
    long FromKey,
    long LastAttemptedKey,
    string Reason);

public class TableExporter
{
    private readonly AppConfig _config;
    private readonly ILogger _logger;
    private readonly BlobContainerClient _containerClient;

    public TableExporter(AppConfig config, string blobConnectionString, ILogger logger)
    {
        _config = config;
        _logger = logger;

        _containerClient = new BlobContainerClient(blobConnectionString, _config.BlobContainerName);
        _containerClient.CreateIfNotExists();
    }

    public async Task ExportTableAsync(TableConfig tableConfig, string sqlConnectionString, CancellationToken ct)
    {
        var tableName = tableConfig.Name;
        var safeTable = SanitizeTableName(tableName);
        var tableLocalFolder = Path.Combine(_config.OutputFolder, safeTable);
        Directory.CreateDirectory(tableLocalFolder);

        _logger.Information(
            "Starting export for table {Table} with key column {KeyColumn}, batch size {BatchSize}",
            tableName, tableConfig.KeyColumn, tableConfig.BatchSize);

        // Manifest & failed chunks
        var manifestBlobClient = _containerClient.GetBlobClient($"{safeTable}/manifest.csv");
        var manifestEntries = new List<ManifestEntry>();
        var failedChunks = new List<FailedChunkInfo>();

        long resumeFromKey = 0;
        int partNumber = 1;
        long alreadyExportedRows = 0;

        // Load existing manifest (if any) to resume
        if (await manifestBlobClient.ExistsAsync(ct))
        {
            _logger.Information("Existing manifest found for table {Table}. Loading for resume...", tableName);
            var download = await manifestBlobClient.DownloadContentAsync(ct);
            var csv = download.Value.Content.ToString();
            using var reader = new StringReader(csv);
            string? line;
            bool isHeader = true;
            while ((line = reader.ReadLine()) is not null)
            {
                if (isHeader)
                {
                    isHeader = false;
                    continue;
                }

                if (string.IsNullOrWhiteSpace(line)) continue;

                var parts = line.Split(',');
                if (parts.Length < 7) continue;

                var entry = new ManifestEntry(
                    TableName: parts[0],
                    PartNumber: int.Parse(parts[1], CultureInfo.InvariantCulture),
                    FromKey: long.Parse(parts[2], CultureInfo.InvariantCulture),
                    ToKey: long.Parse(parts[3], CultureInfo.InvariantCulture),
                    RowCount: long.Parse(parts[4], CultureInfo.InvariantCulture),
                    BlobPath: parts[5],
                    Status: parts[6]);

                manifestEntries.Add(entry);
            }

            var completed = manifestEntries.Where(e => e.Status == "Completed").ToList();
            if (completed.Any())
            {
                resumeFromKey = completed.Max(e => e.ToKey);
                alreadyExportedRows = completed.Sum(e => e.RowCount);
                partNumber = completed.Max(e => e.PartNumber) + 1;

                _logger.Information(
                    "Resume point for table {Table}: last key/offset {LastKey}, completed parts {Parts}, rows {Rows}",
                    tableName, resumeFromKey, completed.Count, alreadyExportedRows);
            }
        }
        else
        {
            _logger.Information("No existing manifest for table {Table}. Starting fresh.", tableName);
        }

        await using var connection = new SqlConnection(sqlConnectionString);
        await connection.OpenAsync(ct);

        long lastKey = resumeFromKey;
        long totalRowsThisRun = 0;

        while (!ct.IsCancellationRequested)
        {
            var chunkFromKey = lastKey + 1;

            try
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                var chunk = await ReadChunkAsync(connection, tableConfig, lastKey, ct);

                if (chunk.RowsCount == 0)
                {
                    _logger.Information(
                        "No more rows for table {Table}. Export completed. Rows this run: {RowsThisRun}, total incl. previous runs: {TotalRows}",
                        tableName, totalRowsThisRun, alreadyExportedRows + totalRowsThisRun);
                    break;
                }

                lastKey = chunk.LastKey;
                totalRowsThisRun += chunk.RowsCount;

                var timestamp = DateTime.UtcNow.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);
                var fileName = $"{safeTable}_{timestamp}_part{partNumber:D5}.parquet";
                var localPath = Path.Combine(tableLocalFolder, fileName);
                var blobPath = $"{safeTable}/{fileName}";

                await WriteParquetAsync(chunk, localPath, ct);

                stopwatch.Stop();

                var overallRows = alreadyExportedRows + totalRowsThisRun;
                _logger.Information(
                    "Table {Table} part {Part}: {Rows} rows → {File}. Elapsed {ElapsedMs} ms. Rows this run: {RowsThisRun}, total approx: {OverallRows}",
                    tableName, partNumber, chunk.RowsCount, localPath, stopwatch.ElapsedMilliseconds,
                    totalRowsThisRun, overallRows);

                await UploadToBlobAsync(safeTable, fileName, localPath, ct);

                // Record success in manifest
                manifestEntries.Add(new ManifestEntry(
                    TableName: tableName,
                    PartNumber: partNumber,
                    FromKey: chunk.FirstKey,
                    ToKey: chunk.LastKey,
                    RowCount: chunk.RowsCount,
                    BlobPath: blobPath,
                    Status: "Completed"));

                partNumber++;
            }
            catch (Exception ex)
            {
                _logger.Error(ex,
                    "Chunk failed for table {Table}, part {Part}, starting from key/offset {FromKey}, lastKey before failure {LastKey}",
                    tableName, partNumber, chunkFromKey, lastKey);

                failedChunks.Add(new FailedChunkInfo(
                    TableName: tableName,
                    PartNumber: partNumber,
                    FromKey: chunkFromKey,
                    LastAttemptedKey: lastKey,
                    Reason: ex.Message));

                manifestEntries.Add(new ManifestEntry(
                    TableName: tableName,
                    PartNumber: partNumber,
                    FromKey: chunkFromKey,
                    ToKey: lastKey,
                    RowCount: 0,
                    BlobPath: string.Empty,
                    Status: "Failed"));

                // break for safety; resume will continue later
                break;
            }
        }

        // Write manifest locally and upload
        var localManifestPath = Path.Combine(tableLocalFolder, "manifest.csv");
        await WriteManifestCsvAsync(localManifestPath, manifestEntries, ct);
        await UploadManifestAsync(safeTable, localManifestPath, ct);

        if (failedChunks.Any())
        {
            var localErrorsPath = Path.Combine(tableLocalFolder, $"errors_{safeTable}.csv");
            await WriteErrorsCsvAsync(localErrorsPath, failedChunks, ct);
            var errorsBlob = _containerClient.GetBlobClient($"{safeTable}/errors_{safeTable}.csv");
            await using var fs = File.OpenRead(localErrorsPath);
            await errorsBlob.UploadAsync(fs, overwrite: true, cancellationToken: ct);

            _logger.Warning("Table {Table} had {Count} failed chunks. See {Blob} for details.",
                tableName, failedChunks.Count, errorsBlob.Name);
        }

        _logger.Information(
            "Finished export for table {Table}. Parts (completed total): {Parts}, rows this run: {RowsThisRun}, total approx rows: {TotalRows}",
            tableName,
            manifestEntries.Count(e => e.Status == "Completed"),
            totalRowsThisRun,
            alreadyExportedRows + totalRowsThisRun);
    }

    private async Task<(List<string> ColumnNames, List<string?>[] Columns, long RowsCount, long FirstKey, long LastKey)>
        ReadChunkAsync(SqlConnection connection, TableConfig tableConfig, long lastKey, CancellationToken ct)
    {
        var tableName = tableConfig.Name;
        var keyColumn = tableConfig.KeyColumn;
        var batchSize = tableConfig.BatchSize;

        var hasKey = !string.Equals(keyColumn, "__NO_KEY__", StringComparison.OrdinalIgnoreCase);

        if (!hasKey)
        {
            // Fallback: OFFSET-based paging, lastKey is used as offset
            var queryNoKey = $@"
    SELECT *
    FROM {tableName}
    ORDER BY (SELECT NULL)
    OFFSET @Offset ROWS
    FETCH NEXT @BatchSize ROWS ONLY;";

            await using var cmd = new SqlCommand(queryNoKey, connection)
            {
                CommandTimeout = 0
            };
            cmd.Parameters.AddWithValue("@BatchSize", batchSize);
            cmd.Parameters.AddWithValue("@Offset", lastKey);

            await using var reader = await cmd.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess, ct);

            if (!reader.HasRows)
            {
                return (new List<string>(), Array.Empty<List<string?>>(), 0, lastKey + 1, lastKey);
            }

            var columnCount = reader.FieldCount;
            var columnNames = new List<string>(columnCount);
            for (int i = 0; i < columnCount; i++)
            {
                columnNames.Add(reader.GetName(i));
            }

            var columnData = new List<string?>[columnCount];
            for (int i = 0; i < columnCount; i++)
            {
                columnData[i] = new List<string?>(capacity: batchSize);
            }

            long rowCount = 0;
            long chunkFirstKey = lastKey + 1;
            long chunkLastKey = lastKey;

            while (await reader.ReadAsync(ct))
            {
                rowCount++;
                chunkLastKey = lastKey + rowCount;

                for (int i = 0; i < columnCount; i++)
                {
                    if (reader.IsDBNull(i))
                    {
                        columnData[i].Add(null);
                    }
                    else
                    {
                        var val = reader.GetValue(i);
                        columnData[i].Add(val?.ToString());
                    }
                }
            }

            return (columnNames, columnData, rowCount, chunkFirstKey, chunkLastKey);
        }
        else
        {
            // Numeric key mode (fast)
            var query = $@"
    SELECT TOP (@BatchSize) *
    FROM {tableName}
    WHERE {keyColumn} > @LastKey
    ORDER BY {keyColumn};";

            await using var cmd = new SqlCommand(query, connection)
            {
                CommandTimeout = 0
            };
            cmd.Parameters.AddWithValue("@BatchSize", batchSize);
            cmd.Parameters.AddWithValue("@LastKey", lastKey);

            await using var reader = await cmd.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess, ct);

            if (!reader.HasRows)
            {
                return (new List<string>(), Array.Empty<List<string?>>(), 0, lastKey + 1, lastKey);
            }

            var columnCount = reader.FieldCount;
            var columnNames = new List<string>(columnCount);
            for (int i = 0; i < columnCount; i++)
            {
                columnNames.Add(reader.GetName(i));
            }

            var columnData = new List<string?>[columnCount];
            for (int i = 0; i < columnCount; i++)
            {
                columnData[i] = new List<string?>(capacity: batchSize);
            }

            long chunkLastKey = lastKey;
            long chunkFirstKey = 0;
            var keyColumnIndex = reader.GetOrdinal(keyColumn);

            long rowCount = 0;

            while (await reader.ReadAsync(ct))
            {
                rowCount++;

                var keyValObj = reader.GetValue(keyColumnIndex);
                if (keyValObj == null || keyValObj is DBNull)
                {
                    throw new InvalidOperationException(
                        $"Key column {keyColumn} contains NULL. Cannot use for streaming.");
                }

                var currentKey = Convert.ToInt64(keyValObj, CultureInfo.InvariantCulture);
                if (rowCount == 1)
                {
                    chunkFirstKey = currentKey;
                }
                if (currentKey > chunkLastKey)
                {
                    chunkLastKey = currentKey;
                }

                for (int i = 0; i < columnCount; i++)
                {
                    if (reader.IsDBNull(i))
                    {
                        columnData[i].Add(null);
                    }
                    else
                    {
                        var val = reader.GetValue(i);
                        columnData[i].Add(val?.ToString());
                    }
                }
            }

            return (columnNames, columnData, rowCount, chunkFirstKey, chunkLastKey);
        }
    }

    private async Task WriteParquetAsync(
        (List<string> ColumnNames, List<string?>[] Columns, long RowsCount, long FirstKey, long LastKey) chunk,
        string path,
        CancellationToken ct)
    {
        var fields = chunk.ColumnNames
            .Select(name => new DataField<string>(name))
            .Cast<Field>()
            .ToArray();
        var schema = new Parquet.Schema.Schema(fields);

        await using var fs = File.Create(path);
        using var parquetWriter = new ParquetWriter(schema, fs);
        using var rowGroupWriter = parquetWriter.CreateRowGroup(chunk.RowsCount);

        for (int i = 0; i < chunk.Columns.Length; i++)
        {
            var field = (DataField<string>)fields[i];
            var dataColumn = new DataColumn(field, chunk.Columns[i].ToArray());
            rowGroupWriter.WriteColumn(dataColumn);
        }
    }

    private async Task UploadToBlobAsync(string safeTable, string fileName, string localPath, CancellationToken ct)
    {
        var blobName = $"{safeTable}/{fileName}";
        var blobClient = _containerClient.GetBlobClient(blobName);

        await RetryHelper.ExecuteWithRetryAsync(
            async () =>
            {
                await using var fileStream = File.OpenRead(localPath);
                await blobClient.UploadAsync(fileStream, overwrite: true, cancellationToken: ct);
            },
            maxRetries: _config.Retry.MaxRetries,
            baseDelay: TimeSpan.FromSeconds(_config.Retry.BaseDelaySeconds),
            logger: _logger,
            operationName: $"Upload:{blobName}");

        _logger.Information("Uploaded {LocalPath} to blob {BlobName}", localPath, blobName);
    }

    private async Task WriteManifestCsvAsync(string path, List<ManifestEntry> entries, CancellationToken ct)
    {
        var sb = new StringBuilder();
        sb.AppendLine("TableName,PartNumber,FromKey,ToKey,RowCount,BlobPath,Status");

        foreach (var e in entries.OrderBy(e => e.PartNumber))
        {
            sb.AppendLine(string.Join(",", new[]
            {
                e.TableName,
                e.PartNumber.ToString(CultureInfo.InvariantCulture),
                e.FromKey.ToString(CultureInfo.InvariantCulture),
                e.ToKey.ToString(CultureInfo.InvariantCulture),
                e.RowCount.ToString(CultureInfo.InvariantCulture),
                e.BlobPath,
                e.Status
            }));
        }

        await File.WriteAllTextAsync(path, sb.ToString(), Encoding.UTF8, ct);
    }

    private async Task UploadManifestAsync(string safeTable, string localManifestPath, CancellationToken ct)
    {
        var manifestBlob = _containerClient.GetBlobClient($"{safeTable}/manifest.csv");
        await using var fs = File.OpenRead(localManifestPath);
        await manifestBlob.UploadAsync(fs, overwrite: true, cancellationToken: ct);

        _logger.Information("Manifest uploaded for table {Table} to blob {BlobName}", safeTable, manifestBlob.Name);
    }

    private async Task WriteErrorsCsvAsync(string path, List<FailedChunkInfo> failedChunks, CancellationToken ct)
    {
        var sb = new StringBuilder();
        sb.AppendLine("TableName,PartNumber,FromKey,LastAttemptedKey,Reason");
        foreach (var f in failedChunks)
        {
            sb.AppendLine(string.Join(",", new[]
            {
                f.TableName,
                f.PartNumber.ToString(CultureInfo.InvariantCulture),
                f.FromKey.ToString(CultureInfo.InvariantCulture),
                f.LastAttemptedKey.ToString(CultureInfo.InvariantCulture),
                f.Reason.Replace(',', ';')
            }));
        }

        await File.WriteAllTextAsync(path, sb.ToString(), Encoding.UTF8, ct);
    }

    private static string SanitizeTableName(string tableName)
        => tableName
            .Replace("[", string.Empty)
            .Replace("]", string.Empty)
            .Replace(".", "_");
}
