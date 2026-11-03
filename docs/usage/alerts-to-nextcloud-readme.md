# AlertsToNextcloud

Export filtered LSST alert data as Parquet files directly to DESY Nextcloud shares, returning read-only URLs for external collaboration.

## Overview

`AlertsToNextcloud` is a Spark application that:

1. Queries the LSST alerts Iceberg table with user-specified filters
2. Streams results as **Parquet files with Zstandard compression** directly to DESY Nextcloud
3. Returns **read-only URLs** for secure data sharing with external collaborators

**Key Feature:** No local file writes - data streams directly from Spark to Nextcloud via WebDAV.

---

## Prerequisites

### Runtime Requirements

- **Java:** 17+
- **Spark:** 4.1.3 (provided at runtime)
- **Dependencies:** All required dependencies are bundled in the JAR (via Iceberg's transitive dependency on parquet-hadoop)

### Nextcloud Requirements

- **DESY Sync & Share** account with WebDAV enabled
- **Read-write share URL** with token (e.g., `https://syncandshare.desy.de/index.php/s/iCa5QFkYo6CB98w`)
- **Read-only share URL** with separate token for consumers

**Note:** DESY Nextcloud supports WebDAV by default. Connection may be slow (official DESY notice).

---

## Installation

### Building from Source

```bash
# Clone the repository
git clone <repository-url>
cd spark-iceberg-kafka

# Build the JAR
mvn clean package

# The JAR will be in target/
ls -la target/spark-iceberg-kafka-image-1.0.0.jar
```

### Using the Fat JAR

The built JAR contains all necessary dependencies (shadow JAR). No additional installation required.

---

## Usage

### Basic Syntax

```bash
spark-submit \
  --class org.lsst.alerts.AlertsToNextcloud \
  target/spark-iceberg-kafka-image-1.0.0.jar \
  [options]
```

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `--nextcloud-base-url` | Base URL of Nextcloud instance | `https://syncandshare.desy.de` |
| `--nextcloud-upload-token` | Token from read-write share URL | `iCa5QFkYo6CB98w` |
| `--nextcloud-download-token` | Token for read-only share URL | `xmbgLKqAqMseNot` |
| `--filename-prefix` | Prefix for output files | `alerts_export_20261102` |

### Query Parameters

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `--where` | SQL WHERE clause for filtering | `diaSource.diaObjectId > 0 and diaSource.reliability > 0.95 and diaSource.visit >= 2026040100000 and diaSource.visit < 2026043000000 and diaSource.psfFlux > 0` | `--where "diaSource.ra BETWEEN 10 AND 20"` |
| `--limit` | Maximum rows to export | None (all matching) | `--limit 10000` |
| `--order-by` | Order by expression | None | `--order-by diaSource.visit DESC` |
| `--schema-exclude` | Fields to exclude from schema | `ssSource,mpc_orbits,cutoutTemplate,cutoutScience,cutoutDifference` | `--schema-exclude ssSource cutoutTemplate` |

### Optional Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--wait` | Keep driver alive after completion (seconds) | 0 | `--wait 60` |

---

## Examples

### Export Recent High-Reliability Alerts

```bash
spark-submit \
  --class org.lsst.alerts.AlertsToNextcloud \
  target/spark-iceberg-kafka-image-1.0.0.jar \
  --where "diaSource.reliability > 0.99 and diaSource.visit >= 2026100100000" \
  --limit 50000 \
  --nextcloud-base-url https://syncandshare.desy.de \
  --nextcloud-upload-token iCa5QFkYo6CB98w \
  --nextcloud-download-token xmbgLKqAqMseNot \
  --filename-prefix recent_high_rel_alerts
```

### Export Specific Field Range

```bash
spark-submit \
  --class org.lsst.alerts.AlertsToNextcloud \
  target/spark-iceberg-kafka-image-1.0.0.jar \
  --where "diaSource.ra BETWEEN 10 AND 50 AND diaSource.dec BETWEEN -10 AND 10" \
  --order-by diaSource.psfFlux DESC \
  --limit 100000 \
  --schema-exclude ssSource mpc_orbits \
  --nextcloud-base-url https://syncandshare.desy.de \
  --nextcloud-upload-token abc123xyz \
  --nextcloud-download-token def456uvw \
  --filename-prefix field_range_export
```

---

## Output

### Console Output

On successful execution, the application prints the list of read-only URLs:

```
INFO: Starting AlertsToNextcloud export
INFO: Query: SELECT ... WHERE diaSource.reliability > 0.99 ...
INFO: Found 4 partitions to process
INFO: Processing partition 0...
INFO: Uploaded partition 0: recent_high_rel_alerts_part-0.parquet
INFO: Processing partition 1...
INFO: Uploaded partition 1: recent_high_rel_alerts_part-1.parquet
INFO: Processing partition 2...
INFO: Uploaded partition 2: recent_high_rel_alerts_part-2.parquet
INFO: Processing partition 3...
INFO: Uploaded partition 3: recent_high_rel_alerts_part-3.parquet
INFO: Export complete. Downloaded URLs:
[
  "https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-0.parquet?download",
  "https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-1.parquet?download",
  "https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-2.parquet?download",
  "https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-3.parquet?download"
]
```

### File Naming Convention

Files are named according to the pattern:
```
{filename-prefix}_part-{partitionId}.parquet
```

Where:
- `{filename-prefix}`: User-specified prefix
- `{partitionId}`: Spark partition number (0-indexed)

---

## Consuming the Data

### With DuckDB

**Prerequisite:** Install and load the `httpfs` extension.

```sql
-- One-time setup
INSTALL httpfs;
LOAD httpfs;

-- Query all exported files as a single table
SELECT * FROM read_parquet([
    'https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-0.parquet?download',
    'https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-1.parquet?download',
    'https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-2.parquet?download',
    'https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-3.parquet?download'
]);

-- Count total rows across all files
SELECT COUNT(*) AS total_rows FROM read_parquet([
    'https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-0.parquet?download',
    'https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-1.parquet?download',
    'https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-2.parquet?download',
    'https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-3.parquet?download'
]);

-- Filter and aggregate
SELECT 
    diaSource.filter_name,
    COUNT(*) as alert_count,
    AVG(diaSource.psfFlux) as avg_flux
FROM read_parquet([
    'https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-*.parquet?download'
])
GROUP BY diaSource.filter_name;
```

**Important:** Glob patterns (`*.parquet`) **do NOT work** over HTTPS. You **must** use explicit URL arrays.

### With Python (pyarrow)

```python
import pyarrow.parquet as pq
import pandas as pd

urls = [
    "https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-0.parquet?download",
    "https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-1.parquet?download",
]

# Read all files into a single DataFrame
dataframes = [pd.read_parquet(url, engine='pyarrow') for url in urls]
combined_df = pd.concat(dataframes, ignore_index=True)

print(f"Loaded {len(combined_df)} rows")
print(combined_df.head())
```

### With Custom HTTP Client

Any HTTP client that supports range requests can read the Parquet files directly:

```bash
# Download a single file
curl -L "https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot/recent_high_rel_alerts_part-0.parquet?download" \
     -o local_copy.parquet

# Then process locally
```

---

## Obtaining Share Tokens

### Creating a Share in DESY Nextcloud

1. Navigate to the folder you want to share in DESY Nextcloud
2. Click the **Share** button (top-right)
3. Click **+** to create a new share
4. Select **Link** as the share type
5. Choose permissions:
   - For upload: **Allow editing** (read-write)
   - For sharing: **Read only** (separate link)
6. Copy the share link

### Extracting Tokens from URLs

**Read-Write URL:**
```
https://syncandshare.desy.de/index.php/s/iCa5QFkYo6CB98w
                                         ^^^^^^^^^^^
                                         Upload Token
```

**Read-Only URL:**
```
https://syncandshare.desy.de/index.php/s/xmbgLKqAqMseNot
                                         ^^^^^^^^^^^
                                         Download Token
```

Use these tokens with the respective parameters:
- `--nextcloud-upload-token iCa5QFkYo6CB98w`
- `--nextcloud-download-token xmbgLKqAqMseNot`

---

## Behavior and Characteristics

### Multi-File Output

The application generates **one Parquet file per Spark partition**. This design choice:

- ✅ Prevents OutOfMemoryError for large datasets (1-10GB)
- ✅ Distributes work across Spark executors
- ✅ Maintains the "no local files" requirement
- ⚠️ Produces multiple files for consumers to manage

### Memory Usage

- **Per partition:** Memory proportional to row group size (configurable, default ~8MB)
- **Driver:** Minimal - only stores URL list, not data
- **Executors:** Buffers one row group at a time per partition

### Performance

- **Network:** Limited by DESY Nextcloud bandwidth (may be slow)
- **CPU:** Moderate - Parquet compression (Zstd) is CPU-intensive
- **Scalability:** Linear with number of partitions

### Reliability

- **Failure handling:** Partial uploads are deleted on failure
- **Retries:** Currently no automatic retries (fail fast)
- **Recovery:** Re-run the export with corrected parameters

---

## Troubleshooting

### Common Issues

#### "Cannot open file" or HTTP 401/403

**Cause:** Invalid upload token or insufficient permissions.

**Solution:**
- Verify the share URL grants write access
- Ensure the token hasn't expired
- Check that the share hasn't been revoked

#### Connection Timeout

**Cause:** Slow DESY network or large partitions.

**Solution:**
- Reduce partition size (more partitions)
- Increase timeout with `--wait` parameter
- Split export into smaller batches

#### OutOfMemoryError

**Cause:** Partitions are too large for available memory.

**Solution:**
- Increase Spark executor memory
- Repartition the dataset before export:
  ```java
  Dataset<Row> repartitioned = alerts.repartition(100);
  ```

#### "Failed to delete partial upload"

**Warning:** Cleanup failed but original error still reported.

**Action:** Manually delete the partial file from Nextcloud WebDAV.

### Debug Mode

Enable verbose logging:

```bash
spark-submit \
  --class org.lsst.alerts.AlertsToNextcloud \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j-debug.properties" \
  target/spark-iceberg-kafka-image-1.0.0.jar \
  [your parameters]
```

Sample `log4j-debug.properties`:
```properties
log4j.rootCategory=DEBUG, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
```

---

## Limitations

### Current Limitations

1. **No glob support for consumers:** DuckDB cannot use `*.parquet` over HTTPS (technical limitation of HTTP protocol)
2. **No automatic retries:** Transient failures require manual intervention
3. **Sequential partition processing:** Partitions processed one at a time (could be parallelized)
4. **No progress tracking:** No progress bar or percentage complete

### Known Issues

- None identified in current version

---

## Security Considerations

### Token Security

- Share tokens grant access to anyone who possesses them
- **Never commit tokens to version control**
- **Never log tokens** (mask in logs)
- Consider using environment variables instead of command-line arguments

### Data Exposure

- Files are accessible via read-only URL to anyone with the token
- Ensure tokens are shared securely with intended recipients only
- Revoke shares when no longer needed

### Best Practices

```bash
# Better: Use environment variables
export NEXTCLOUD_UPLOAD_TOKEN=iCa5QFkYo6CB98w
export NEXTCLOUD_DOWNLOAD_TOKEN=xmbgLKqAqMseNot

spark-submit \
  --class org.lsst.alerts.AlertsToNextcloud \
  target/spark-iceberg-kafka-image-1.0.0.jar \
  --nextcloud-base-url https://syncandshare.desy.de \
  --nextcloud-upload-token $NEXTCLOUD_UPLOAD_TOKEN \
  --nextcloud-download-token $NEXTCLOUD_DOWNLOAD_TOKEN \
  --filename-prefix my_export
```

---

## Comparison with AlertsToKafka

| Feature | AlertsToKafka | AlertsToNextcloud |
|---------|---------------|-------------------|
| **Destination** | Kafka topic | Nextcloud share |
| **Format** | Avro | Parquet with Zstd |
| **Compression** | Configurable | Zstd (fixed) |
| **Storage** | Kafka brokers | Nextcloud storage |
| **Access** | Kafka consumers | Anyone with URL |
| **Multi-file** | Single stream | One file per partition |
| **Dependencies** | Kafka clients | None (uses HttpClient) |
| **Use Case** | Real-time streaming | Batch export for sharing |

---

## FAQ

### Why multiple files instead of one?

Generating one file per partition prevents OutOfMemoryError for large datasets. Spark drivers typically have limited memory, and collecting 10GB of data would exhaust heap space. By processing partitions independently, each executor only holds one partition's data in memory at a time.

### Can I get a single file?

Currently no. The streaming approach requires partitioning. If you absolutely need a single file:
1. Export to multiple files
2. Download all files
3. Merge locally with Parquet tools
4. Re-upload as single file

We recommend embracing the multi-file approach, as DuckDB handles it seamlessly.

### Why doesn't glob work with DuckDB over HTTPS?

DuckDB maintainer Hannes explained: *"There is no (standard, reliable) way to list files on a HTTP server, hence we cannot glob there."* HTTP servers don't provide directory listing APIs like local filesystems do. The workaround is to provide an explicit list of URLs.

### How do I know which fields are available?

Run with default parameters to see the schema, or check the Iceberg alerts table schema:

```bash
spark-submit \
  --class org.lsst.alerts.AlertsToKafka \
  target/spark-iceberg-kafka-image-1.0.0.jar \
  --where "1=0"  # No rows, just schema
```

### Can I cancel a running export?

Yes, use standard Spark cancellation mechanisms. Partial files will remain on Nextcloud until manually deleted.

---

## Support

### Reporting Issues

Report bugs and feature requests via [GitHub Issues](<repository-url>/issues) (once repository is set up).

Include:
- Command line used
- Error messages
- Spark version
- Java version
- Nextcloud instance details (if applicable)

### Getting Help

For usage questions:
- Check this README
- Review the [Design Document](../design/alerts-to-nextcloud-design.md)
- Consult DuckDB documentation for consumer-side questions

---

## License

This project is licensed under the Apache License 2.0 - see [LICENSE](../../LICENSE) for details.

---

## Changelog

### Unreleased

- Initial implementation of AlertsToNextcloud
- Streaming Parquet to Nextcloud via WebDAV
- Multi-file output (one per partition)
- DuckDB consumer compatibility

---

## See Also

- [Design Document](../design/alerts-to-nextcloud-design.md) - Architecture and decisions
- [Implementation Plan](../implementation/alerts-to-nextcloud-implementation-plan.md) - Engineering roadmap
- [AlertsToKafka.java](../../src/main/java/org/lsst/alerts/AlertsToKafka.java) - Reference implementation
- [DESY Sync & Share Documentation](https://it.desy.de/services/storage_services/desy_sync__share/) - Official DESY docs
- [DuckDB HTTPFS Extension](https://duckdb.org/docs/current/core_extensions/httpfs/https) - DuckDB remote file access
