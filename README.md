# ParquetExporter

.NET 10 console app to export very large SQL Server tables (up to multi-TB) to Parquet files in Azure Blob Storage.

## Features

- Chunked reading by numeric key column (4TB-safe).
- Fallback OFFSET-based paging for tables without a numeric key.
- Per-table manifest (`manifest.csv`) with:
  - part number
  - key/offset range
  - row count
  - blob path
  - status (Completed/Failed)
- Resume support:
  - Reads existing manifest from blob
  - Continues from last completed key/offset.
- Error tracking:
  - `errors_<table>.csv` listing failed chunks after retries.
- Parquet file per chunk: `Table_yyyyMMddHHmmss_part00001.parquet`.
- Folder-per-table in Blob Storage: `<TableName>/file.parquet`.
- Parallel export across tables (`MaxDegreeOfParallelism`).
- Serilog logging (console, file, Elasticsearch).
- Retry with exponential backoff.
- Config via `config/<ENV>/appsettings.json` (ConfigMap-friendly).
- Dockerfile + AKS CronJob YAML (heavy node pool example).
- Discovery mode to list tables and primary keys for quick config.

## Project Layout

```text
ParquetExporter/
  src/
    ParquetExporter/
      ParquetExporter.csproj
      Program.cs
      Models/
      Services/
      config/
        Development/appsettings.json
        Production/appsettings.json
  k8s/
    configmap.yaml
    secrets.yaml
    cronjob-heavy.yaml
  Dockerfile
  README.md
  LICENSE
  .gitignore
```

## Usage

### 1. Discover tables & keys

Set `SQL_CONNECTION_STRING` in your environment and run:

```bash
dotnet run --project src/ParquetExporter -- discover
```

This will print a recommended `AppConfig.Tables` section to paste into `config/Production/appsettings.json`.  
It will warn if:

- A table has **no primary key** → uses `KeyColumn = "__NO_KEY__"` and OFFSET paging.
- A primary key is **not numeric** → still works, but uses slower paging.

### 2. Configure tables

Edit `config/Production/appsettings.json` under `AppConfig.Tables`:

```json
"Tables": [
  { "Name": "dbo.Customers", "KeyColumn": "CustomerId", "BatchSize": 50000 },
  { "Name": "dbo.Orders",   "KeyColumn": "OrderId",    "BatchSize": 50000 },
  { "Name": "dbo.Logs",     "KeyColumn": "__NO_KEY__", "BatchSize": 50000 }
]
```

- Use a **numeric key** for best performance (`int`, `bigint`, etc.).
- Use `"__NO_KEY__"` to enable OFFSET paging for tables without a numeric key.

### 3. Run export locally

Set these environment variables:

- `SQL_CONNECTION_STRING`
- `BLOB_CONNECTION_STRING`
- `DOTNET_ENVIRONMENT` = `Development` or `Production`

Then run:

```bash
dotnet run --project src/ParquetExporter
```

The app will:

- Export each table in parallel.
- Write Parquet chunks to local `/tmp/parquet-out/<TableName>/...`.
- Upload each chunk to Blob Storage.
- Maintain `manifest.csv` and `errors_<table>.csv` in Blob.

### 4. Docker

Build & push:

```bash
docker build -t myacr.azurecr.io/parquet-exporter:1.0.0 .
docker push myacr.azurecr.io/parquet-exporter:1.0.0
```

### 5. AKS Deployment

1. Create namespace (if needed):

```bash
kubectl create namespace data-jobs
```

2. Apply ConfigMap & Secret:

```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secrets.yaml
```

3. Apply CronJob (heavy-node pool example):

```bash
kubectl apply -f k8s/cronjob-heavy.yaml
```

This CronJob:

- Runs weekly (`0 2 * * 0`) at 02:00 UTC.
- Uses a nodeSelector `nodepool: batch-heavy` so you can target large nodes.
- Requests/limits more CPU & memory for this heavy job.

## iPhone / GitHub Notes

You can upload this repo to GitHub directly from your iPhone:

1. Download the ZIP.
2. Go to GitHub in Safari → New Repository (`ParquetExporter`).
3. Use **Add file → Upload files** to upload the unzipped files/folders.
4. Commit, and your repo is ready.

## License

MIT License. See `LICENSE` file.
