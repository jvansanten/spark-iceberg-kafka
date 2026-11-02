# AlertsToNextcloud Implementation Plan

## Overview

This document outlines the step-by-step engineering plan for implementing the `AlertsToNextcloud` class, based on the design described in [alerts-to-nextcloud-design.md](../design/alerts-to-nextcloud-design.md).

---

## Phase 1: Foundation (Estimated: 1-2 days)

### Step 1: Project Setup

**Objective:** Prepare the development environment.

**Tasks:**
- [ ] Create new Java class: `src/main/java/org/lsst/alerts/AlertsToNextcloud.java`
- [ ] Copy relevant utilities from `AlertsToKafka.java` (query building, argument parsing pattern)
- [ ] Update `pom.xml` mainClass to support both classes (or create separate module)
- [ ] Set up IDE with proper dependencies resolved

**Verification:**
```bash
# Verify Iceberg and Parquet dependencies are available
mvn dependency:tree | grep -E "(iceberg|parquet)"
```

**Deliverables:**
- New class skeleton
- Build configuration updated

---

### Step 2: Implement Custom OutputFile for HTTP Streaming

**Objective:** Create the streaming Parquet writer adapter.

**Tasks:**
- [ ] Create inner class `ParquetBufferedWriter` implementing `org.apache.parquet.io.OutputFile`
- [ ] Implement `PositionOutputStream` wrapper that tracks position manually
- [ ] Handle `supportsBlockSize()` and `defaultBlockSize()` appropriately
- [ ] Add proper exception handling and resource cleanup

**Code Location:** Inner class within `AlertsToNextcloud`

**Reference Implementation:**
```java
private static class ParquetBufferedWriter implements OutputFile {
    private final BufferedOutputStream out;
    
    ParquetBufferedWriter(BufferedOutputStream out) {
        this.out = Objects.requireNonNull(out);
    }
    
    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return new TrackingPositionOutputStream(out);
    }
    
    @Override
    public boolean supportsBlockSize() {
        return false;
    }
    
    @Override
    public long defaultBlockSize() {
        return 0;
    }
    
    private static class TrackingPositionOutputStream extends PositionOutputStream {
        private final BufferedOutputStream delegate;
        private long position = 0;
        
        TrackingPositionOutputStream(BufferedOutputStream delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public long getPos() {
            return position;
        }
        
        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            position++;
        }
        
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            position += len;
        }
        
        @Override
        public void flush() throws IOException {
            delegate.flush();
        }
        
        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
```

**Verification:**
- [ ] Unit test: Write small dataset to `ByteArrayOutputStream`, verify Parquet validity
- [ ] Test: Round-trip data through streaming writer

**Deliverables:**
- `ParquetBufferedWriter` implementation
- Unit tests for streaming functionality

---

### Step 3: Nextcloud WebDAV Client

**Objective:** Implement HTTP PUT functionality for Nextcloud uploads.

**Tasks:**
- [ ] Create utility class or methods for WebDAV operations
- [ ] Implement HTTP PUT with Basic Auth (username=token, password="")
- [ ] Add required header: `X-Requested-With: XMLHttpRequest`
- [ ] Implement DELETE for cleanup on failure
- [ ] Handle HTTP response codes appropriately

**Implementation Approach:** Use Java 11+ `HttpClient` (already in use by `AlertsToKafka`)

**Code Sketch:**
```java
private static HttpClient createNextcloudClient() {
    return HttpClient.newBuilder()
            .connectTimeout(Duration.ofMinutes(5))
            .build();
}

private static HttpRequest buildPutRequest(
        String webDavUrl, 
        String token, 
        long contentLength,
        InputStream data) {
    return HttpRequest.newBuilder()
            .uri(URI.create(webDavUrl))
            .header("Authorization", basicAuth(token, ""))
            .header("X-Requested-With", "XMLHttpRequest")
            .PUT(HttpRequest.BodyPublishers.ofInputStream(() -> data))
            .build();
}

private static String basicAuth(String username, String password) {
    return "Basic " + Base64.getEncoder()
            .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
}

private static void deleteFile(String webDavUrl, String token) throws IOException, InterruptedException {
    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(webDavUrl))
            .header("Authorization", basicAuth(token, ""))
            .header("X-Requested-With", "XMLHttpRequest")
            .DELETE()
            .build();
    HttpResponse<Void> response = HttpClient.newHttpClient()
            .send(request, HttpResponse.BodyHandlers.discarding());
    if (response.statusCode() != 204 && response.statusCode() != 200) {
        throw new IOException("Failed to delete: " + response);
    }
}
```

**Verification:**
- [ ] Unit test: Mock HTTP server to verify PUT/DELETE requests
- [ ] Test: Verify authentication headers are correct

**Deliverables:**
- WebDAV client methods
- HTTP request/response handling

---

## Phase 2: Core Functionality (Estimated: 2-3 days)

### Step 4: Argument Parsing

**Objective:** Parse command-line arguments similar to `AlertsToKafka`.

**Tasks:**
- [ ] Port argument parsing logic from `AlertsToKafka.Arguments`
- [ ] Remove Kafka-specific parameters
- [ ] Add Nextcloud-specific parameters:
  - `--nextcloud-base-url`
  - `--nextcloud-upload-token`
  - `--nextcloud-download-token`
  - `--filename-prefix`
- [ ] Add validation for new parameters
- [ ] Preserve existing query parameters (where, limit, order-by, schema-exclude)

**Default Values:**
- filename-prefix: Derived from query or timestamp (e.g., `alerts_{yyyyMMdd_HHmmss}`)

**Verification:**
- [ ] Unit tests for argument parsing edge cases
- [ ] Help text displays correctly

**Deliverables:**
- Complete argument parser
- Validation logic

---

### Step 5: Query Execution

**Objective:** Execute query against Iceberg alerts table.

**Tasks:**
- [ ] Port query building logic from `AlertsToKafka.buildQuery()`
- [ ] Create SparkSession with appropriate configuration
- [ ] Execute query and obtain Dataset<Row>
- [ ] Convert to RDD for partition-level processing

**Code Sketch:**
```java
SparkSession spark = SparkSession.builder()
        .appName("AlertsToNextcloud")
        .getOrCreate();

String query = buildQuery(options, schemaFields);
Dataset<Row> alerts = spark.sql(query);

// Convert to RDD for foreachPartition
JavaRDD<Row> rowRDD = alerts.toLocalIterator().asScala().toSeq().rdd().toJavaRDD();
// Or better: alerts.rdd().toJavaRDD()
```

**Verification:**
- [ ] Integration test: Query existing alerts table
- [ ] Test: Verify filtering works correctly

**Deliverables:**
- Query execution logic
- Dataset to RDD conversion

---

### Step 6: Partition Processing with foreachPartition

**Objective:** Process each partition independently, streaming to Nextcloud.

**Tasks:**
- [ ] Implement `foreachPartition` to distribute work across executors
- [ ] For each partition:
  - Get partition index
  - Create unique filename: `{prefix}_part-{index}.parquet`
  - Open HTTP connection to Nextcloud
  - Create Parquet writer with custom OutputFile
  - Write all rows from partition
  - Close writer and verify upload
  - On failure: DELETE partial file, throw exception
  - Collect resulting URL
- [ ] Aggregate all URLs from all partitions

**Code Structure:**
```java
List<String> downloadUrls = alerts.rdd().toJavaRDD()
        .foreachPartition(iterator -> {
            int partitionId = TaskContext.get().partitionId();
            String filename = options.filenamePrefix + "_part-" + partitionId + ".parquet";
            String webDavUrl = buildWebDavUrl(options, filename);
            String downloadUrl = buildDownloadUrl(options, filename);
            
            try {
                uploadPartition(iterator, webDavUrl, options.uploadToken, filename);
                partitionUrls.add(downloadUrl);
            } catch (Exception e) {
                cleanupPartialUpload(webDavUrl, options.uploadToken);
                throw new RuntimeException("Failed to upload partition " + partitionId, e);
            }
        });
```

**Important Consideration:** `foreachPartition` doesn't return values directly. Need to use accumulator or transform to RDD of URLs.

**Better Approach:**
```java
JavaRDD<String> urlRDD = alerts.rdd().toJavaRDD()
        .mapPartitions(iterator -> {
            int partitionId = TaskContext.get().partitionId();
            String filename = options.filenamePrefix + "_part-" + partitionId + ".parquet";
            String webDavUrl = buildWebDavUrl(options, filename);
            String downloadUrl = buildDownloadUrl(options, filename);
            
            try {
                uploadPartition(iterator, webDavUrl, options.uploadToken);
                return Collections.singletonList(downloadUrl).iterator();
            } catch (Exception e) {
                cleanupPartialUpload(webDavUrl, options.uploadToken);
                throw e;
            }
        });

List<String> downloadUrls = urlRDD.collect();
```

**Verification:**
- [ ] Unit test: Mock partition iterator
- [ ] Integration test: Multiple partitions with small dataset

**Deliverables:**
- Partition processing logic
- foreachPartition implementation

---

### Step 7: Parquet Writing Logic

**Objective:** Write partition data as Parquet to HTTP OutputStream.

**Tasks:**
- [ ] Determine schema from Dataset<Row>
- [ ] Convert Row to Parquet Group or GenericRecord
- [ ] Create ParquetWriter with:
  - Custom OutputFile (ParquetBufferedWriter)
  - Zstd compression
  - Appropriate row group size
- [ ] Write all rows from partition
- [ ] Close writer to flush and finalize

**Schema Conversion Challenge:**
Rows need to be converted to Parquet-compatible format. Two approaches:

**Option A: Use Spark's internal Parquet writer**
- Leverage Spark's existing Parquet support
- May require more complex integration

**Option B: Manual conversion to Parquet Groups**
- Convert Row → GenericRecord or Group
- More control, more code

**Recommended: Option A** - Use Spark's Parquet facilities where possible.

Alternatively, use Iceberg's Parquet writer utilities.

**Code Sketch (simplified):**
```java
MessageType schema = convertSparkSchemaToParquetSchema(dataset.schema());

OutputStream httpOut = openHttpOutputStream(webDavUrl, token);
BufferedOutputStream bufferedOut = new BufferedOutputStream(httpOut);
OutputFile outputFile = new ParquetBufferedWriter(bufferedOut);

ParquetWriter<Group> writer = new ParquetWriter<>(
        outputFile,
        schema,
        CompressionCodecFactory.getCodec(ZstdCodec.class),
        ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.DEFAULT_PAGE_SIZE
);

while (iterator.hasNext()) {
    Row row = iterator.next();
    Group group = convertRowToGroup(row, schema);
    writer.write(group);
}

writer.close();
bufferedOut.flush();
```

**Verification:**
- [ ] Test: Write partition, read back with DuckDB
- [ ] Test: Verify Zstd compression applied

**Deliverables:**
- Parquet writing logic
- Schema conversion utilities

---

## Phase 3: Polish (Estimated: 1 day)

### Step 8: Error Handling and Cleanup

**Objective:** Robust error handling with proper cleanup.

**Tasks:**
- [ ] Wrap all upload operations in try-catch
- [ ] On failure: DELETE partial file from Nextcloud
- [ ] Propagate meaningful error messages
- [ ] Handle HTTP errors (4xx, 5xx) appropriately
- [ ] Add timeout configurations

**Error Categories:**
- Network errors: Retry? Or fail fast? (Recommend: Fail fast, let caller retry)
- Authentication errors: Invalid token, fail immediately
- Quota errors: Nextcloud storage full, fail with clear message
- Internal errors: Bug in our code, fail with stack trace

**Cleanup Strategy:**
```java
try {
    uploadPartition(...);
} catch (Exception e) {
    try {
        deleteFile(webDavUrl, token);
    } catch (Exception cleanupError) {
        logger.warn("Failed to cleanup partial upload: " + cleanupError);
    }
    throw e;
}
```

**Verification:**
- [ ] Test: Inject failures, verify cleanup attempted
- [ ] Test: Verify error messages are informative

**Deliverables:**
- Comprehensive error handling
- Cleanup logic

---

### Step 9: Logging and Observability

**Objective:** Provide visibility into the export process.

**Tasks:**
- [ ] Add SLF4J logging
- [ ] Log key events: start, partition progress, completion
- [ ] Log warnings for recoverable issues
- [ ] Include timing metrics where useful
- [ ] Log URL count and total bytes (if trackable)

**Log Levels:**
- INFO: Job start/end, partition counts, total URLs
- DEBUG: Individual file uploads, HTTP details
- WARN: Failed cleanup, retried operations
- ERROR: Fatal errors

**Verification:**
- [ ] Test: Verify logs at different levels
- [ ] Test: Verify no sensitive data (tokens) logged

**Deliverables:**
- Logging configuration
- Instrumentation code

---

### Step 10: Documentation and Examples

**Objective:** Make the class usable and discoverable.

**Tasks:**
- [ ] Add JavaDoc to public methods
- [ ] Document parameters in usage text
- [ ] Add example invocations
- [ ] Document DuckDB consumption pattern

**See:** README.md (separate document)

---

## Phase 4: Testing (Estimated: 1-2 days)

### Unit Tests

**Scope:**
- [ ] Argument parsing (valid, invalid, edge cases)
- [ ] URL construction (various base URLs and tokens)
- [ ] ParquetBufferedWriter with mock OutputStream
- [ ] HTTP client request building
- [ ] Row to Parquet conversion
- [ ] Error handling and cleanup

**Framework:** JUnit 5

**Setup:**
```xml
<dependency>
    <groupId>org.junit.jupiter</groupId>
    <artifactId>junit-jupiter</artifactId>
    <version>5.9.2</version>
    <scope>test</scope>
</dependency>
```

---

### Integration Tests

**Scope:**
- [ ] End-to-end with local WebDAV server (e.g., [rclone serving WebDAV](https://rclone.org/commands/rclone_serve_webdav/))
- [ ] Spark local mode with test data
- [ ] Multiple partitions
- [ ] Large dataset (1GB+) if feasible
- [ ] Error scenarios (invalid token, server unavailable)

**Test Infrastructure:**
```bash
# Start test WebDAV server
rclone serve webdav ./test-data --addr :8080 &

# Run integration tests
mvn verify -Pintegration-tests
```

---

### Consumer Validation Tests

**Scope:**
- [ ] DuckDB connectivity test
- [ ] Verify `read_parquet([url1, url2, ...])` works
- [ ] Schema consistency across files
- [ ] Data integrity (row counts match)

**Test Script:**
```python
# test_duckdb_consumption.py
import duckdb
import subprocess
import os

# Run AlertsToNextcloud
result = subprocess.run([...], capture_output=True, text=True)
urls = parse_urls_from_output(result.stdout)

# Connect DuckDB
conn = duckdb.connect()
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")

# Query all files
query = f"SELECT COUNT(*) FROM read_parquet({urls})"
total_rows = conn.execute(query).fetchone()[0]

assert total_rows == expected_count, f"Expected {expected_count}, got {total_rows}"
```

---

## Phase 5: Deployment (Estimated: 0.5 day)

### Packaging

**Tasks:**
- [ ] Update pom.xml with new main class or create fat JAR
- [ ] Build with `mvn clean package`
- [ ] Verify JAR contains all dependencies

**Fat JAR Plugin:**
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>3.5.0</version>
    <executions>
        <execution>
            <phase>package</phase>
            <goals>
                <goal>shade</goal>
            </goals>
        </execution>
    </executions>
</plugin>
```

---

### Submission

**Tasks:**
- [ ] Deploy JAR to artifact repository
- [ ] Update deployment manifests
- [ ] Tag release in git

---

## Estimated Timeline

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| Phase 1: Foundation | 1-2 days | Custom OutputFile, WebDAV client, arg parsing |
| Phase 2: Core | 2-3 days | Query exec, partition processing, Parquet writing |
| Phase 3: Polish | 1 day | Error handling, logging, docs |
| Phase 4: Testing | 1-2 days | Unit, integration, consumer validation |
| Phase 5: Deployment | 0.5 day | Package, deploy |
| **Total** | **6-8.5 days** | Complete implementation |

---

## Resource Requirements

### Human Resources
- 1 Senior Java/Spark engineer (primary)
- Optional: 1 QA engineer for test automation

### Infrastructure
- Development environment with Java 17+, Maven
- Spark cluster for testing (local mode initially)
- Test WebDAV server (rclone, minio, or Nextcloud test instance)
- DESY Nextcloud access for final validation

### Tools
- IDE (IntelliJ IDEA or VS Code)
- Maven 3.8+
- Docker (for test services)
- DuckDB CLI for consumer validation

---

## Validation Checkpoints

| Checkpoint | Criteria | Owner |
|------------|----------|-------|
| CP1: Streaming Feasibility | ParquetBufferedWriter writes valid Parquet to OutputStream | Engineer |
| CP2: WebDAV Integration | Successful PUT/DELETE to test WebDAV server | Engineer |
| CP3: Partition Processing | Multiple partitions generate correct files | Engineer |
| CP4: End-to-End | Full pipeline works with test data | Engineer |
| CP5: DuckDB Consumption | DuckDB can query generated files | Engineer |
| CP6: Production Readiness | All tests pass, documentation complete | Team |

---

## Risk Mitigation

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Parquet streaming doesn't work | Low | High | Prototype early, reference StackOverflow solution |
| WebDAV auth issues | Medium | High | Test with DESY docs, verify token auth |
| Memory issues with large partitions | Medium | Medium | Monitor partition sizes, configure row groups |
| Network timeouts | Medium | Medium | Configurable timeouts, retry logic |
| DuckDB incompatibility | Low | Medium | Early consumer validation testing |

---

## Next Steps

1. **Immediate:** Begin Phase 1 (Foundation) - Create class skeleton and custom OutputFile
2. **Day 1 Goal:** Have ParquetBufferedWriter working with mock OutputStream
3. **Week 1 Goal:** Complete Phases 1-2 (Core functionality)
4. **Week 2 Goal:** Complete Phases 3-5 (Polish, testing, deployment)

---

## Appendices

### Appendix A: Glossary

- **WebDAV:** Web Distributed Authoring and Versioning - HTTP extension for file management
- **Zstd:** Zstandard compression algorithm - fast compression with good ratios
- **Partition:** Subset of Spark RDD data, processed independently
- **Row Group:** Parquet organizational unit, contains contiguous rows

### Appendix B: References

- [Design Document](../design/alerts-to-nextcloud-design.md)
- [AlertsToKafka.java](../../src/main/java/org/lsst/alerts/AlertsToKafka.java)
- [DESY WebDAV Docs](https://it.desy.de/services/storage_services/desy_sync__share/configuration_for_webdav_with_desy_sync_and_share/index_eng.html)
