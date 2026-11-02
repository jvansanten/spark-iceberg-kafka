# AlertsToNextcloud Design Document

## Overview

This document describes the design for a new Spark class, `AlertsToNextcloud`, which executes queries against the LSST alerts Iceberg table, streams the results as Parquet files directly to a DESY Nextcloud share, and returns read-only URLs for consumer access.

### Motivation

**Primary Use Case:** Secure data sharing with external collaborators who need read-only access to filtered alert datasets.

The existing `AlertsToKafka` class demonstrates a pattern for querying alerts and distributing them via Kafka. This new class adapts that pattern for Nextcloud-based distribution, enabling:
- External collaboration without granting internal system access
- Fine-grained filtering of sensitive astronomical alert data
- Self-service data export for authorized personnel

### Success Criteria

1. ✅ No local file writes (hard requirement)
2. ✅ Direct streaming to Nextcloud via WebDAV
3. ✅ Parquet format with Zstandard compression
4. ✅ Read-only URL generation for consumers
5. ✅ Compatible with DuckDB's `read_parquet([url1, url2, ...])`
6. ✅ Handles medium-large datasets (1-10GB) efficiently
7. ✅ Proper error handling with cleanup of partial uploads

---

## Architecture

### Component Diagram

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Spark Driver  │────▶│  Spark Executors │────▶│  Nextcloud      │
│                 │     │                  │     │  (DESY Sync&Share)│
├─────────────────┤     ├──────────────────┤     ├─────────────────┤
│ 1. Parse args  │     │ 2. Query         │     │ WebDAV          │
│ 2. Build query │     │    Iceberg       │     │ /public.php/dav/│
│ 3. Submit job   │     │    alerts table  │     │ endpoint        │
│ 4. Collect URLs│     │ 3. foreachPartition│    │                 │
└─────────────────┘     │    ├── Partition 0│◄───▶│ PUT file_part-0│
                       │    │   └── Stream ►│    │ Parquet         │
                       │    ├── Partition 1│◄───▶│ PUT file_part-1│
                       │    │   └── Stream ►│    │ Parquet         │
                       │    └── ...        │    │                 │
                       └──────────────────┘     └─────────────────┘
                                                          ▲
                                                          │
                              ┌───────────────────────────┴───────────────────────────┐
                              │                                                   │
                              ▼                                                   ▼
                    ┌─────────────────────┐           ┌─────────────────────┐
                    │   Consumer (DuckDB) │           │   Consumer (Other)  │
                    │                     │           │                     │
                    │ SELECT * FROM       │           │ Custom HTTP client  │
                    │ read_parquet([      │           │                     │
                    │   url1, url2, ...    │           │                     │
                    │ ]);                 │           │                     │
                    └─────────────────────┘           └─────────────────────┘
```

### Data Flow

1. **Input:** Query parameters (WHERE clause, limit, order-by) + Nextcloud share configuration
2. **Processing:** Spark queries Iceberg alerts table, distributes across partitions
3. **Output:** Each executor partition:
   - Opens HTTP connection to Nextcloud WebDAV endpoint
   - Creates Parquet writer wrapped around HTTP OutputStream
   - Streams partition data as Parquet with Zstd compression
   - Closes connection, verifies upload
4. **Result:** List of read-only URLs (one per partition) returned to caller

---

## Key Design Decisions

### 1. Multi-File Output Strategy

**Decision:** Generate one Parquet file per Spark partition.

**Rationale:**
- Avoids OutOfMemoryError on driver for large datasets (1-10GB)
- Enables distributed processing across executors
- Satisfies "no local files" requirement
- Compatible with DuckDB via explicit URL list

**Alternatives Considered:**
- Single file: Collect all data to driver → Risk of OOM for 10GB datasets
- Temporary local files: Violates hard requirement

**Trade-offs:**
- Pros: Memory-safe, scalable, distributed
- Cons: Multiple files for consumers to manage

**Mitigation:** Return ordered list of URLs; consumers use `read_parquet([url1, url2, ...])`

### 2. Streaming Parquet to HTTP

**Decision:** Use custom `OutputFile` implementation wrapping HTTP `OutputStream`.

**Rationale:**
- Standard ParquetWriter requires seekable output (filesystem)
- Custom wrapper tracks position manually, satisfying Parquet's interface
- Verified feasible via StackOverflow solutions and Apache Flink's `StreamOutputFile`

**Implementation Sketch:**
```java
class ParquetBufferedWriter implements OutputFile {
    private final BufferedOutputStream out;
    
    @Override
    public PositionOutputStream create(long blockSizeHint) {
        return new PositionOutputStream() {
            private long pos = 0;
            // Implement write(), getPos(), flush(), close()
            // Track position manually
        };
    }
    
    @Override
    public boolean supportsBlockSize() { return false; }
    
    @Override
    public long defaultBlockSize() { return 0; }
}
```

**Considerations:**
- ParquetWriter buffers row groups internally
- Configure row group size to balance memory vs. I/O efficiency
- Close writer to flush all data and finalize file

### 3. Nextcloud WebDAV Protocol

**Decision:** Use WebDAV via `/public.php/dav/` endpoint with token-based authentication.

**Rationale:**
- DESY officially supports WebDAV for Sync & Share
- Public share tokens enable authentication without passwords
- Standard HTTP PUT for file uploads

**Connection Details:**
- Endpoint: `https://syncandshare.desy.de/public.php/dav/{filename}`
- Authentication: Basic auth with username={share_token}, password=""
- Required header: `X-Requested-With: XMLHttpRequest`
- Method: PUT

### 4. DuckDB Consumer Compatibility

**Decision:** Return list of explicit URLs; consumers use `read_parquet([url1, url2, ...])`.

**Rationale:**
- DuckDB maintainer confirmed: **"There is no (standard, reliable) way to list files on a HTTP server, hence we cannot glob there"**
- Glob patterns (`*.parquet`) **do NOT work** over HTTPS
- Explicit URL arrays **DO work** with `read_parquet()` function

**Consumer Pattern:**
```sql
INSTALL httpfs;
LOAD httpfs;

-- Works:
SELECT * FROM read_parquet([
    'https://syncandshare.desy.de/index.php/s/TOKEN/file_part-0.parquet',
    'https://syncandshare.desy.de/index.php/s/TOKEN/file_part-1.parquet'
]);

-- Does NOT work:
SELECT * FROM 'https://syncandshare.desy.de/index.php/s/TOKEN/*.parquet';
```

### 5. Error Handling

**Decision:** Delete partial uploads on failure, then throw exception.

**Rationale:**
- Prevents corrupted/incomplete files from being accessible
- Maintains data integrity for consumers
- Clear failure semantics

**Implementation:**
- Try-catch around each partition upload
- On exception: HTTP DELETE to partially uploaded file
- Propagate exception to caller

---

## Interface Design

### Parameters

The class replaces Kafka-specific parameters with Nextcloud equivalents:

| Category | AlertsToKafka | AlertsToNextcloud |
|----------|---------------|-------------------|
| Query | `--where`, `--limit`, `--order-by` | Same |
| Schema | `--schema-registry`, `--subject`, `--schema-exclude` | Same |
| Destination | `--topic`, `--kafka-*` | **Removed** |
| Nextcloud | - | `--nextcloud-base-url`, `--nextcloud-upload-token`, `--nextcloud-download-token`, `--filename-prefix` |
| Debug | `--wait` | Same |

**New Parameters:**
- `--nextcloud-base-url <url>`: Base URL of Nextcloud instance (e.g., `https://syncandshare.desy.de`)
- `--nextcloud-upload-token <token>`: Token extracted from read-write share URL
- `--nextcloud-download-token <token>`: Token for read-only share URL
- `--filename-prefix <prefix>`: Prefix for output files (we append `_part-{n}.parquet`)

### Return Value

Returns a list of read-only URLs (Strings):
```java
List<String> getDownloadUrls();
```

Each URL format: `{base-url}/index.php/s/{download-token}/{filename-prefix}_part-{partitionId}.parquet?download`

*Note:* The `?download` parameter requires verification with the test URL provided.

---

## Dependencies

### Existing (Available)
- ✅ `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0`
  - Transitively includes `org.apache.parquet:parquet-hadoop:1.17.1`
  - Provides: `ParquetWriter`, `OutputFile`, `PositionOutputStream`, `ZstdCodec`
- ✅ `org.apache.spark:spark-sql_2.13:4.1.3` (provided)
- ✅ Java 17 (for HttpClient)

### New Dependencies
**None required.** All necessary classes available via transitive dependencies.

---

## Trade-offs and Risks

### Memory Management

**Risk:** ParquetWriter buffers row groups internally before flushing.

**Mitigation:**
- One file per partition spreads memory load across executors
- Configure row group size appropriately (e.g., 1MB-8MB)
- Monitor partition sizes; warn if approaching memory limits

**Recommendation:** Default row group size of 8MB balances memory and I/O efficiency for 1-10GB datasets.

### Network Performance

**Risk:** DESY warns "connection can be relatively slow"; network-bound writes may be slow.

**Mitigation:**
- Use buffered OutputStream (already planned)
- Compress with Zstd (reduces data volume)
- Parallel uploads from multiple executors
- Progress reporting for monitoring

### Failure Recovery

**Risk:** Network interruption leaves partial file on Nextcloud.

**Mitigation:**
- DELETE partial file on failure (before throwing exception)
- Idempotent operations (same filename will overwrite)
- Transactional semantics: all files succeed or none are accessible

**Limitation:** Race condition window between upload completion and URL return; extremely unlikely in practice.

### Schema Evolution

**Risk:** Changes in alerts table schema may cause issues.

**Mitigation:**
- Inherit schema handling from AlertsToKafka
- Expose schema exclusion parameters
- Document schema requirements

### Security

**Risk:** Read-write tokens exposed in logs or process listings.

**Mitigation:**
- Tokens passed via command-line arguments (visible in process list)
- Recommend using environment variables or config files in production
- Document security considerations for operators

---

## Rollout Plan

### Phase 1: Development & Testing
1. Implement `AlertsToNextcloud` class
2. Unit tests (parameter parsing, URL construction, OutputFile impl)
3. Integration tests with local WebDAV server (e.g., minio or test Nextcloud)
4. Consumer validation with DuckDB

### Phase 2: Staging
1. Deploy to staging environment
2. Test with DESY Nextcloud (non-production share)
3. Validate with realistic dataset sizes (1-5GB)
4. Performance benchmarking

### Phase 3: Production
1. Deploy to production
2. Monitor initial usage
3. Iterate based on feedback

---

## Open Questions

1. **Read-only URL format:** Does `?download` parameter work, or is another format needed? *(Test with provided URL)*
2. **Row group size:** Optimal value for 1-10GB partition sizes
3. **DELETE on failure:** Verify Nextcloud WebDAV DELETE works with same auth
4. **Connection pooling:** Should we reuse HTTP connections across partitions?

---

## References

- [DESY Sync & Share WebDAV Documentation](https://it.desy.de/services/storage_services/desy_sync__share/configuration_for_webdav_with_desy_sync_and_share/index_eng.html)
- [Nextcloud WebDAV API Docs](https://docs.nextcloud.com/server/latest/developer_manual/client_apis/WebDAV/basic.html)
- [DuckDB HTTPFS Extension](https://duckdb.org/docs/current/core_extensions/httpfs/https)
- [DuckDB Multiple Files](https://duckdb.org/docs/current/data/multiple_files/overview)
- [StackOverflow: Parquet to Byte Stream](https://stackoverflow.com/questions/40089689/parquet-writer-to-buffer-or-byte-stream)
- [Apache Parquet Java API](https://parquet.apache.org/documentation/latest/)
