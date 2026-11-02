# AlertsToNextcloud Test Plan

**Status:** Draft
**Branch:** `feature/alerts-to-nextcloud`
**Related documents:**
- [Design Document](../design/alerts-to-nextcloud-design.md)
- [Implementation Plan](../implementation/alerts-to-nextcloud-implementation-plan.md)
- [Usage README](../usage/alerts-to-nextcloud-readme.md)

---

## 1. Overview and Scope

This document defines the test strategy for `AlertsToNextcloud`, a Spark application that
queries the LSST alerts Iceberg table and streams filtered results as Zstd-compressed
Parquet files directly to a DESY Nextcloud share over WebDAV, returning read-only URLs
for consumers.

The strategy covers four layers:

| Layer | Purpose | Runs where | Cost |
|-------|---------|------------|------|
| **Unit tests** | Verify pure logic (arg parsing, URL building, Parquet writing, HTTP request construction, row conversion) | Local JVM, no network, no Spark cluster | Seconds |
| **Integration tests** | Verify end-to-end behaviour against a local WebDAV server and local Spark/Iceberg | Docker Compose + local Spark | Minutes |
| **Consumer validation** | Prove DuckDB/pyarrow can read the generated files | Local DuckDB / Python | Minutes |
| **Production smoke test** | One real export against DESY Nextcloud | Manual, credentialed | Manual |

**Out of scope:** load/soak testing at production scale, Kafka path (covered separately by
`AlertsToKafka` tests), Nextcloud server administration.

### Test objectives

Map directly to the success criteria in the design document:

1. No local file is ever written to disk during upload.
2. Parquet produced is valid and readable by an independent reader (DuckDB/pyarrow).
3. Zstd compression is actually applied.
4. WebDAV auth (`Basic` with `token:` and `X-Requested-With` header) is correct.
5. Read-only URLs returned are well-formed and resolve for consumers.
6. A failed partition upload is cleaned up with a `DELETE`, and the failure is propagated
   (no silent degradation).
7. Row/record counts are preserved end-to-end (no data loss).

---

## 2. Unit Test Framework Setup

### 2.1 Framework recommendation

Use **JUnit 5 (Jupiter)** with **Mockito** for test doubles and **AssertJ** (optional) for
fluent assertions. JUnit 5 is recommended over JUnit 4 because:

- It is the current standard and works cleanly with Java 17.
- `@ParameterizedTest` suits argument-parsing and URL-construction tables.
- `@TempDir`, `assertThrows`, and nested test classes make local-server tests cleaner.
- Better Maven Surefire integration with `maven-surefire-plugin` 3.x.

For HTTP testing prefer the JDK's built-in **`com.sun.net.httpserver.HttpServer`** (no extra
dependency bundled into the fat JAR) or **`mockwebserver`** if richer request inspection is
needed. `HttpServer` is preferred here to keep the dependency surface small.

Parquet round-trip assertions use **`parquet-avro` / `parquet-hadoop`** already available
transitively, plus **DuckDB** only in integration/consumer tests.

### 2.2 Maven dependency snippet

Add to `pom.xml` (current project has **no** test dependencies):

```xml
<properties>
    <junit.version>5.10.2</junit.version>
    <mockito.version>5.12.0</mockito.version>
    <assertj.version>3.25.3</assertj.version>
    <maven.surefire.version>3.2.5</maven.surefire.version>
</properties>

<dependencies>
    <!-- Existing dependencies unchanged -->

    <!-- Unit testing -->
    <dependency>
        <groupId>org.junit.jupiter</groupId>
        <artifactId>junit-jupiter</artifactId>
        <version>${junit.version}</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>org.junit.jupiter</groupId>
        <artifactId>junit-jupiter-params</artifactId>
        <version>${junit.version}</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>org.mockito</groupId>
        <artifactId>mockito-core</artifactId>
        <version>${mockito.version}</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>org.mockito</groupId>
        <artifactId>mockito-junit-jupiter</artifactId>
        <version>${mockito.version}</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>org.assertj</groupId>
        <artifactId>assertj-core</artifactId>
        <version>${assertj.version}</version>
        <scope>test</scope>
    </dependency>

    <!-- Reading back Parquet in unit tests (transitively available via Iceberg,
         declared explicitly so the test scope is stable) -->
    <dependency>
        <groupId>org.apache.parquet</groupId>
        <artifactId>parquet-avro</artifactId>
        <version>1.17.1</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.avro</groupId>
        <artifactId>avro</artifactId>
        <version>1.11.3</version>
        <scope>test</scope>
    </dependency>
</dependencies>

<build>
    <plugins>
        <!-- existing compiler/jar plugins unchanged -->
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-surefire-plugin</artifactId>
            <version>${maven.surefire.version}</version>
            <configuration>
                <!-- Spark + JDK 17 needs these opens for local-mode Spark tests -->
                <argLine>
                    --add-opens=java.base/java.lang=ALL-UNNAMED
                    --add-opens=java.base/java.nio=ALL-UNNAMED
                    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
                    --add-opens=java.base/java.util=ALL-UNNAMED
                    -Duser.timezone=UTC
                </argLine>
            </configuration>
        </plugin>
    </plugins>
</build>
```

> **Note:** Because the project currently uses no `<scope>` on several runtime deps and
> later intends a shade/fat JAR, keep all test dependencies `test`-scoped so they never
> leak into the distributed artifact.

### 2.3 Directory structure

```
src/
└── test/
    ├── java/
    │   └── org/
    │       └── lsst/
    │           └── alerts/
    │               ├── AlertsToNextcloudArgumentsTest.java
    │               ├── NextcloudUrlBuilderTest.java
    │               ├── ParquetBufferedWriterTest.java
    │               ├── WebDavClientTest.java
    │               ├── RowToParquetConversionTest.java
    │               ├── UploadCleanupTest.java
    │               └── support/                 # shared test helpers (not test classes)
    │                   ├── LocalWebDavServer.java
    │                   ├── ParquetTestUtils.java
    │                   ├── TestSchemas.java
    │                   └── SparkTestBase.java
    ├── resources/
    │   ├── log4j2-test.properties
    │   ├── sample-alerts.parquet
    │   └── webdav-responses/
    │       ├── put-201-created.txt
    │       ├── put-401-unauthorized.txt
    │       └── delete-404-notfound.txt
    └── python/
        ├── validate_duckdb.py
        └── requirements.txt
```

Test classes follow `*Test.java` so Surefire picks them up by default. Helpers live under
`support/` and are not named `*Test`, so they are not executed as tests.

---

## 3. Unit Test Cases

Each subsection lists the class under test, the scenarios, the technique, and the expected
result. Argument parsing and URL construction are pure and should reach **high coverage**;
streaming and HTTP code rely on fakes/local servers.

### 3.1 Arguments parsing tests

**Target:** the `Arguments` inner class (ported from `AlertsToKafka.Arguments`).
**File:** `AlertsToNextcloudArgumentsTest.java`

Use `@ParameterizedTest` for table-driven cases.

| # | Scenario | Input | Expected |
|---|----------|-------|----------|
| A1 | All required Nextcloud options supplied | `--nextcloud-base-url … --nextcloud-upload-token … --nextcloud-download-token … --filename-prefix …` | Fields populated exactly |
| A2 | Defaults applied when query options omitted | no `--where/--limit/--order-by` | `where` = `DEFAULT_WHERE`, `limit` = null, `orderBy` = null |
| A3 | `--where` override | `--where "diaSource.ra > 10"` | `where` equals the provided string verbatim |
| A4 | `--limit` parsing | `--limit 0`, `--limit 50000` | Integer parsed; `0` retained (means "all" in query builder) |
| A5 | Invalid `--limit` | `--limit abc` | `NumberFormatException` (wrapped or not — assert exception type) |
| A6 | `--order-by` recorded | `--order-by diaSource.visit DESC` | `orderBy` set verbatim |
| A7 | `--schema-exclude` multi-value | `--schema-exclude ssSource mpc_orbits` | Set = {ssSource, mpc_orbits}; defaults replaced |
| A8 | `--schema-exclude` missing values | `--schema-exclude` at end / followed by `--wait` | `IllegalArgumentException("Missing values for --schema-exclude")` |
| A9 | Default schema-exclude | (no flag) | 5 default fields present |
| A10 | Missing value for a flag | `--nextcloud-upload-token` (last token) | `IllegalArgumentException("Missing value for …")` |
| A11 | Unknown option | `--not-a-flag` | `IllegalArgumentException("Unknown option: …")` |
| A12 | Required Nextcloud options absent | omit `--nextcloud-base-url` | Validation failure (fail-fast; assert message names the missing flag) |
| A13 | `--wait` parsing | `--wait 60` | `waitSeconds == 60` |
| A14 | Kafka-only flags rejected/removed | `--topic x` | `IllegalArgumentException` (Kafka params were intentionally removed) |
| A15 | Trailing slash in base URL | `--nextcloud-base-url https://host/` | Stored as-is for parsing; normalization asserted in URL builder tests |
| A16 | `--help` | `--help` | Prints usage and exits 0 (see note below) |

> **Note on `--help`/`System.exit`:** `printUsage()` calls `System.exit(0)`, which kills the
> Surefire JVM. Refactor `parse()` to return a sentinel (e.g., a `HelpRequested` result) or
> inject an exit handler so A16 is testable. Alternatively assert help text via a separate
> method and mark this test `@Disabled` until refactored. **Flag this for the implementer.**

### 3.2 URL construction tests

**Target:** `buildWebDavUrl(...)` and `buildDownloadUrl(...)`.
**File:** `NextcloudUrlBuilderTest.java`

URL format per design/README:
- Upload (WebDAV): `{base}/public.php/dav/{filename}` (auth via token)
- Download (read-only): `{base}/index.php/s/{downloadToken}/{filename}?download`

| # | Scenario | Input | Expected |
|---|----------|-------|----------|
| U1 | Standard upload URL | base=`https://syncandshare.desy.de`, file=`exp_part-0.parquet` | `https://syncandshare.desy.de/public.php/dav/exp_part-0.parquet` |
| U2 | Standard download URL | base + token=`xmbg…`, file | `…/index.php/s/xmbg…/exp_part-0.parquet?download` |
| U3 | Base URL with trailing slash | `https://host/` | `https://host/public.php/dav/…` (single slash, no `//`) |
| U4 | Filename with characters needing encoding (space, `#`, `?`) | prefix=`my export` | Path segments are percent-encoded; `?download` still literal |
| U5 | Partition index appended correctly | partitionId 0,1,10 | `_part-0.parquet`, `_part-1.parquet`, `_part-10.parquet` |
| U6 | Token with special characters | token containing `+`, `/` | Token preserved/encoded so URL parses; `URI.create` does not throw |
| U7 | Prefix containing a slash | prefix=`a/b` | Either rejected by validation or encoded — assert chosen policy |
| U8 | Deterministic output | same inputs twice | Identical strings (no timestamp leakage into URL builder) |
| U9 | HTTPS scheme enforced | base=`http://host` | Either accepted or validation error — assert policy (production is HTTPS) |
| U10 | Empty prefix | prefix=`` | Validation error from argument stage; URL builder behaviour documented |

Use `URI.create(url)` and assert `getPath()`/`getQuery()` rather than raw string equality
where encoding matters, to avoid brittle assertions.

### 3.3 ParquetBufferedWriter tests

**Target:** `ParquetBufferedWriter` + `TrackingPositionOutputStream`.
**File:** `ParquetBufferedWriterTest.java`

The critical property: the tracked position must equal the number of bytes actually written
(the delegate is a `BufferedOutputStream`, so `flush()`/`close()` must not corrupt position).

| # | Scenario | Technique | Expected |
|---|----------|-----------|----------|
| P1 | `getPos()` after `write(int)` | Write 1 byte, assert `getPos()==1` | 1 |
| P2 | `getPos()` after `write(byte[],off,len)` | `write(buf, 2, 5)` | +5 |
| P3 | Mixed writes accumulate | interleave single/array writes | sum of written bytes |
| P4 | Position independent of buffering | write, do **not** flush, assert pos; then flush, assert unchanged | pos tracks logical bytes, not flushed bytes |
| P5 | `close()` closes delegate | write to `ByteArrayOutputStream`, close writer, then delegate.toString() length == bytes | delegate closed; content size == pos |
| P6 | `flush()` delegates | use Mockito spy on delegate; verify `flush()` called | delegate flushed |
| P7 | `supportsBlockSize()` returns false | call directly | `false` |
| P8 | `defaultBlockSize()` returns 0 | call directly | `0` |
| P9 | `create(long)` returns a fresh stream | call twice | distinct instances; independent positions |
| P10 | `create()` null/`Objects.requireNonNull` | construct with `null` | `NullPointerException` |
| P11 | End-to-end validity | write a small `ParquetWriter<Group>` over it into a `ByteArrayOutputStream`, then read back with `ParquetReader`/`AvroParquetReader` | row count and values match |
| P12 | Zstd codec applied | write via the production `ParquetWriter` config, inspect footer metadata | codec == `ZSTD` |
| P13 | IOException propagation | delegate that throws on write | exception surfaces unchanged, position not advanced for failed write |

P11 is the **CP1 streaming-feasibility checkpoint** in the implementation plan — it must pass
before integration testing begins.

### 3.4 HTTP client tests

**Target:** `createNextcloudClient()`, `buildPutRequest()`, `basicAuth()`, `deleteFile()`.
**File:** `WebDavClientTest.java`
**Technique:** local `com.sun.net.httpserver.HttpServer` bound to an ephemeral port, plus
Mockito where a static helper is unit-testable in isolation.

| # | Scenario | Expectation |
|---|----------|-------------|
| H1 | `basicAuth("token","")` | Exactly `"Basic " + base64("token:")` — verify empty password, UTF-8 |
| H2 | PUT request carries `Authorization` | Server captures header; equals expected `Basic …` |
| H3 | PUT request carries `X-Requested-With: XMLHttpRequest` | Header present, value exact |
| H4 | PUT method and path | Server sees `PUT /public.php/dav/file_part-0.parquet` |
| H5 | Body streamed | Server reads body; byte count/checksum matches what was sent |
| H6 | Content-Length handling | Verify either `Content-Length` set from known size **or** chunked transfer is accepted; assert what server observes |
| H7 | 2xx → success | 201/204 response ⇒ method returns normally |
| H8 | 401/403 → fail fast | `IOException`/custom exception including status code; no retry loop |
| H9 | 5xx → fail fast | Exception includes `500` and body snippet |
| H10 | DELETE sends correct method/headers | Server sees `DELETE`, auth + `X-Requested-With` |
| H11 | DELETE accepts 204 and 200 | Both treated as success |
| H12 | DELETE rejects other codes | 404 ⇒ exception mentioning status |
| H13 | Timeout configured | Client built with non-zero connect timeout (assert via behaviour or config constant) |
| H14 | Body publisher lazy supplier | `BodyPublishers.ofInputStream(() -> in)` does not read stream until send; verify with a stream whose `read` sets a flag |
| H15 | Tokens never logged | Capture logger output during PUT/DELETE; assert token substring absent |

HTTP tests must assert on **status codes**, never on error message text (per review rubric:
errors checked against stable identifiers).

### 3.5 Row to Parquet conversion tests

**Target:** `convertSparkSchemaToParquetSchema()` and `convertRowToGroup()` (or the chosen
Spark-native equivalent).
**File:** `RowToParquetConversionTest.java`

The design leaves the conversion approach open (Option A: Spark's Parquet facilities;
Option B: manual Row → `Group`/`GenericRecord`). These tests are written against the
**observable contract** so either option can satisfy them.

| # | Scenario | Expectation |
|---|----------|-------------|
| C1 | Primitive types | int/long/double/boolean/string map to correct Parquet types and round-trip values |
| C2 | Null values | nulls preserved (not coerced to 0/empty) |
| C3 | Decimal / timestamp types | mapped correctly; read-back equality with tolerance for timestamp precision |
| C4 | Binary / nested struct | struct field (e.g., `diaSource`) round-trips as a nested group |
| C5 | Array / map (if in schema) | preserved as Parquet LIST/MAP |
| C6 | Schema exclusion respected | excluded fields absent from produced Parquet schema entirely |
| C7 | Field ordering | column order matches schema field order (DuckDB is order-tolerant but consumers rely on names) |
| C8 | Empty partition | zero rows produces a valid Parquet file with schema and 0 rows |
| C9 | Large-ish batch | N rows (e.g., 10k) written then read back; count and a sample checksum match |
| C10 | Type mismatch guard | converting a Row whose value type contradicts schema fails loudly (ClassCastException / explicit error), not silently |
| C11 | Non-nullable constraint | if schema declares required fields, a null produces a clear error |

C8 and C11 matter in production: a partition with all rows filtered out still uploads an
empty-but-valid file, and null handling bugs silently corrupt data consumers rely on.

---

## 4. Integration Test Cases

Integration tests require: local Spark (or the Iceberg REST catalog), a local WebDAV server,
and Docker Compose for the server. They are gated behind a Maven profile so `mvn test`
stays fast and hermetic.

### 4.1 Local WebDAV server setup

Two options; **rclone is recommended** because it is a single static binary and matches the
implementation plan, while MinIO needs an S3-compatible WebDAV gateway.

**Option A — rclone (recommended):**

```bash
# docker-compose.test.yml
services:
  webdav:
    image: rclone/rclone:latest
    command: >
      serve webdav /data
      --addr :8080
      --user testuser
      --pass testpass
      --vfs-cache-mode off
    volumes:
      - ./target/webdav-data:/data
    ports:
      - "8080:8080"
```

```bash
docker compose -f docker-compose.test.yml up -d
# smoke-check
curl -u testuser:testpass -X PUT --data-binary @pom.xml http://localhost:8080/pom.xml
curl -u testuser:testpass http://localhost:8080/ | head
docker compose -f docker-compose.test.yml down -v
```

> **Auth mapping note:** `AlertsToNextcloud` sends Basic auth `username=token, password=""`.
> rclone's `--user/--pass` expects a real password. For tests, configure rclone with a user
> whose password is empty, or run a tiny stub WebDAV handler that accepts the exact
> `Basic base64("token:")` scheme. **Do not** let this difference mask real auth bugs —
> mirror the production scheme (`token:` with empty password) exactly in the test server.

**Option B — MinIO with `--webdav`:** heavier; use only if S3 semantics are also needed.

**Option C — in-process stub:** a `LocalWebDavServer` helper built on
`com.sun.net.httpserver.HttpServer` that implements `PUT`, `DELETE`, and `GET` over a temp
directory, and asserts the expected auth header. This is the most faithful and fastest for
auth assertions; use it for the automated CI path.

`LocalWebDavServer` helper responsibilities:
- start/stop on an ephemeral port
- expose the base URL
- record all received requests (method, path, headers, body digest)
- emulate 401 for wrong tokens, 507 for quota-full, 500/503 for fault injection
- support `failNextPutWith(status)` and `failNextDeleteWith(status)`

### 4.2 Gated Maven profile

```xml
<profiles>
    <profile>
        <id>integration-tests</id>
        <build>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-failsafe-plugin</artifactId>
                    <version>3.2.5</version>
                    <executions>
                        <execution>
                            <goals>
                                <goal>integration-test</goal>
                                <goal>verify</goal>
                            </goals>
                        </execution>
                    </executions>
                </plugin>
            </plugins>
        </build>
    </profile>
</profiles>
```

Integration tests named `*IT.java` run under Failsafe with `mvn verify -Pintegration-tests`.

### 4.3 End-to-end test with a small dataset

**File:** `AlertsToNextcloudEndToEndIT.java`

Setup:
1. Start a local SparkSession in `local[2]` mode.
2. Create an Iceberg (or plain Parquet-backed) temp table `lsst.alerts` with the alerts schema
   and a known number of rows (e.g., 100 rows across 4 partitions).
3. Start `LocalWebDavServer`.
4. Invoke `AlertsToNextcloud` logic (or `spark-submit` against the built fat JAR) with
   `--limit`, `--where`, `--nextcloud-base-url http://localhost:PORT`,
   `--nextcloud-upload-token testtoken`, `--filename-prefix e2e`.

Assertions:
| # | Assertion |
|---|-----------|
| E1 | Process exits 0 |
| E2 | Exactly one file per partition uploaded (`e2e_part-{0..3}.parquet`) |
| E3 | Returned URL list length == partition count |
| E4 | Each returned URL matches the documented download format |
| E5 | Sum of rows across uploaded files == number of rows matching `--where` (no loss/duplication) |
| E6 | No file exists on the local filesystem after the run (design hard requirement) — assert the working dir is untouched and no temp `.parquet` files appear |
| E7 | All uploaded bodies are valid Parquet (read back with DuckDB) |
| E8 | Zstd codec confirmed in each file's metadata |

E5 and E6 are the two most valuable assertions — data integrity and the "no local writes" hard
requirement.

### 4.4 Error scenario tests

**File:** `AlertsToNextcloudErrorIT.java`

| # | Injected fault | Expected behaviour |
|---|----------------|--------------------|
| X1 | PUT returns 401 (bad token) | Job fails fast with auth error; no partial file left; error identifies 401 |
| X2 | PUT returns 507 (quota full) | Job fails with clear "storage full" style message |
| X3 | PUT returns 500 on one partition, others fine | Job fails (no silent partial success); the surviving partition and failed partition both reported |
| X4 | PUT fails mid-body (stream error) | Failure propagates; a `DELETE` is attempted for the partial file |
| X5 | DELETE on cleanup returns 404 | Cleanup failure is logged as WARN, original error still thrown (never swallowed) |
| X6 | DELETE on cleanup returns 500 | Same as X5 |
| X7 | Server unreachable / connection refused | Fail fast after connect timeout; clear message |
| X8 | Slow server exceeding timeout | Timeout exception; cleanup attempted |
| X9 | Empty result set (0 rows) | Behaviour defined: either zero files + empty URL list, or one empty file — assert the chosen contract explicitly |
| X10 | Partition fails after some rows written | No corrupt file remains accessible; DELETE was called |

Criteria: **fail fast**, cleanup attempted, original exception preserved (use
`assertThatThrownBy(...).hasCauseInstanceOf(...)`), and **errors identified by status code**,
not message string.

### 4.5 Large dataset test (optional / manual)

Not run in CI by default. Purpose: validate memory behaviour and multi-partition scaling.

| # | Parameter | Value |
|---|-----------|-------|
| L1 | Dataset size | ~1 GB (e.g., 5M rows, or synthetic wide rows) |
| L2 | Partitions | ≥ 8 |
| L3 | Driver heap | default (assert no OOM on driver) |
| L4 | Executor heap | bounded (e.g., 2g) |
| L5 | Row group size | default (~8 MB); also test 1 MB and 16 MB |
| L6 | Assertions | all files valid; row count preserved; peak driver heap does **not** scale with dataset size; wall-clock per partition roughly linear |

Record results in a benchmark table appended to this document or a sibling
`docs/test/alerts-to-nextcloud-benchmarks.md`.

---

## 5. DuckDB Consumer Validation

This layer proves the design's core consumer promise: `read_parquet([url1, url2, ...])`
works over HTTP(S) and glob does not.

### 5.1 Setup script

`scripts/duckdb-setup.sql`:

```sql
INSTALL httpfs;
LOAD httpfs;

-- Required for DESY Nextcloud/self-signed or corporate TLS; leave commented by default.
-- SET enable_server_cert_verification = false;
```

`src/test/python/requirements.txt`:

```
duckdb>=1.4
pandas>=2.2
pyarrow>=17
requests>=2.32
pytest>=8.2
```

Optional TLS note: DuckDB's `httpfs` uses its own CA bundle. For DESY production the
certificate chain must validate; do **not** disable verification to make tests pass — treat a
TLS failure as a real finding.

### 5.2 Test queries

Against a local stub server exposing the uploaded files at `http://localhost:PORT/<file>`:

```sql
-- 1. Explicit URL array (must work)
SELECT COUNT(*) FROM read_parquet([
  'http://localhost:PORT/e2e_part-0.parquet',
  'http://localhost:PORT/e2e_part-1.parquet'
]);

-- 2. Schema shape
DESCRIBE SELECT * FROM read_parquet('http://localhost:PORT/e2e_part-0.parquet');

-- 3. Row count per file (cross-check partitioning)
SELECT COUNT(*) FROM read_parquet('http://localhost:PORT/e2e_part-0.parquet');

-- 4. Filter / aggregate over nested struct field
SELECT diaSource.filter_name, COUNT(*) AS n, AVG(diaSource.psfFlux) AS avg_flux
FROM read_parquet(['http://localhost:PORT/e2e_part-0.parquet',
                   'http://localhost:PORT/e2e_part-1.parquet'])
GROUP BY diaSource.filter_name;

-- 5. Glob must FAIL (negative test — documents the limitation)
-- SELECT * FROM 'http://localhost:PORT/*.parquet';   -- expected: error
```

Also run **the same queries over the real `?download` DESY URL** during the production smoke
test to confirm the `?download` suffix behaves with httpfs.

### 5.3 Python validation script template

`src/test/python/validate_duckdb.py`:

```python
#!/usr/bin/env python3
"""Validate AlertsToNextcloud output with DuckDB.

Usage:
    python validate_duckdb.py \
        --base-url "${NEXTCLOUD_BASE_URL:-http://localhost:8080}" \
        --prefix e2e \
        --partitions 4 \
        --expected-rows 100
"""
from __future__ import annotations

import argparse
import sys

import duckdb


def build_urls(base_url: str, prefix: str, partitions: int, download_suffix: str) -> list[str]:
    base = base_url.rstrip("/")
    return [f"{base}/{prefix}_part-{i}.parquet{download_suffix}" for i in range(partitions)]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--partitions", type=int, required=True)
    parser.add_argument("--expected-rows", type=int, required=True)
    parser.add_argument("--download-suffix", default="",
                        help="Use '?download' for DESY read-only URLs")
    args = parser.parse_args()

    urls = build_urls(args.base_url, args.prefix, args.partitions, args.download_suffix)
    print("Validating URLs:")
    for u in urls:
        print(f"  - {u}")

    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    url_list = "[" + ", ".join(f"'{u}'" for u in urls) + "]"

    total = con.execute(f"SELECT COUNT(*) FROM read_parquet({url_list})").fetchone()[0]
    print(f"Total rows via DuckDB: {total}")
    assert total == args.expected_rows, (
        f"Row count mismatch: expected {args.expected_rows}, got {total}"
    )

    # Per-file counts must sum to total (detects silent truncation)
    per_file = []
    for u in urls:
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{u}')").fetchone()[0]
        per_file.append(n)
    print(f"Per-file rows: {per_file}")
    assert sum(per_file) == total, "Per-file counts do not sum to total"

    # Schema sanity: a known nested field must exist
    cols = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet({url_list})").fetchall()}
    assert any(c.lower().startswith("diasource") for c in cols), (
        f"Expected a diaSource field, got columns: {sorted(cols)}"
    )

    # Negative test: glob over HTTP must NOT work
    try:
        con.execute("SELECT COUNT(*) FROM read_parquet("
                    f"'{args.base_url.rstrip('/')}/{args.prefix}_part-*.parquet')")
        print("WARNING: HTTP glob unexpectedly succeeded; DuckDB behaviour changed.",
              file=sys.stderr)
    except duckdb.Error:
        print("OK: HTTP glob correctly rejected")

    print("VALIDATION PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

For DESY production smoke tests, wrap with `pytest` and mark network tests so they can be
skipped in CI:

```python
# src/test/python/test_desy_consumer.py
import os
import pytest

@pytest.mark.skipif(not os.environ.get("DESY_RUN_SMOKE"),
                    reason="set DESY_RUN_SMOKE=1 to run against DESY Nextcloud")
def test_desy_urls_readable():
    ...
```

---

## 6. Test Data Preparation

### 6.1 In-memory / generated data (unit + small integration)

Deterministic synthetic rows cover all type paths without depending on the real catalog:

| Dataset | Rows | Purpose |
|---------|------|---------|
| `tinyEdges` | ~20 | nulls, min/max ints, empty strings, unicode, nested struct |
| `smallUniform` | 100 | e2e row-count checks across 4 partitions |
| `wideRows` | 1k | nested struct with many fields, schema-exclude coverage |
| `nullHeavy` | 500 | fields entirely null / partially null |
| `typeMatrix` | ~30 | one row per supported Parquet type |

Build via `TestSchemas` helpers so schema and data stay in sync with the alerts schema:

```java
// support/TestSchemas.java (sketch)
static StructType alertsSchema() { /* struct diaSource { … }, diaObject { … } */ }
static Row diaSourceRow(long id, String filterName, double psfFlux) { … }
```

### 6.2 JSON fixtures

`src/test/resources/webdav-responses/` holds canned response bodies for errors so HTTP tests
can assert behaviour without a live server. Fixtures must include status codes as filenames
(e.g., `put-401-unauthorized.txt`) and be referenced by tests that need to parse an error body.

### 6.3 Realistic sample (integration, optional)

For schema-fidelity testing against the real catalog, export a small slice once and commit it
as `src/test/resources/sample-alerts.parquet` (cap at a few MB). Guidelines:
- Strip any sensitive/identifiable fields if the data is restricted.
- Record the source query and date in a `sample-alerts.README.md` sibling.
- Prefer deterministic generation over checked-in real data for CI.

### 6.4 Data hygiene rules

- **Never** commit real upload/download tokens (even test ones that look real).
- Use obviously fake tokens: `testuploadtoken0123`, `testdownloadtoken456`.
- Keep fixtures < 1 MB each to keep the repo lean.
- All timestamps in UTC (`-Duser.timezone=UTC` in Surefire).

---

## 7. Running Tests

### Unit tests (fast, hermetic, CI default)

```bash
mvn test
```

### Unit tests with coverage

```bash
mvn test jacoco:report -Dmaven.test.failure.ignore=false
# report: target/site/jacoco/index.html
```

### Integration tests (requires Docker + local Spark)

```bash
docker compose -f docker-compose.test.yml up -d
mvn verify -Pintegration-tests
docker compose -f docker-compose.test.yml down -v
```

### Consumer validation only

```bash
python -m pip install -r src/test/python/requirements.txt
python src/test/python/validate_duckdb.py \
  --base-url http://localhost:8080 --prefix e2e --partitions 4 --expected-rows 100
```

### Full local pipeline

```bash
mvn clean package
docker compose -f docker-compose.test.yml up -d
mvn verify -Pintegration-tests
python src/test/python/validate_duckdb.py --base-url http://localhost:8080 \
  --prefix e2e --partitions 4 --expected-rows 100
docker compose -f docker-compose.test.yml down -v
```

### CI integration

Extend `.github/workflows/` with a `test` job (and mirror in `.gitlab-ci.yml`) that runs
`mvn test` on every push and `mvn verify -Pintegration-tests` (with Docker) on PRs. Cache
`~/.m2/repository`. Publish the Surefire/Failsafe reports as artifacts. **No** production
credentials or DESY smoke tests in CI.

---

## 8. Expected Failures and Edge Cases

| # | Edge case | Expected handling | Test |
|---|-----------|-------------------|------|
| F1 | `--help` calls `System.exit` | Refactor for testability; otherwise test is skipped | A16 |
| F2 | Empty result set | Defined contract (empty URL list or one empty file); documented | E9/X9/C8 |
| F3 | Filename with spaces/special chars | Percent-encoded; survives round-trip | U4 |
| F4 | Base URL trailing slash | Normalized to single slash | U3 |
| F5 | rclone auth ≠ production token auth | Use stub server mirroring `token:` scheme | 4.1 |
| F6 | DuckDB glob over HTTP | Must fail; negative test asserts this | 5.2, 5.3 |
| F7 | DESY TLS chain / slow network | Real finding if it fails; not masked by disabling verification | 5.1 |
| F8 | `?download` suffix behaviour with httpfs | Verify in production smoke test; document result | 5.2 |
| F9 | Partial upload left after crash (SIGKILL) | Cleanup cannot run; documented as known limitation + manual cleanup | 4.4, README |
| F10 | All-null partition / required-field null | Clear error (fail fast) | C11 |
| F11 | Token leakage in logs | Asserted absent | H15 |
| F12 | No local file writes | Asserted via working-dir inspection | E6 |
| F13 | Partition id collisions (retried tasks) | Same filename overwrites (idempotent) — documented; verify no duplicate URL entries | E3 |
| F14 | Very large single partition | Memory pressure; mitigated by row-group size; covered by L-tests | 4.5 |
| F15 | Concurrent exports sharing a prefix | Files collide/overwrite; document that prefixes must be unique | review note |

### Pass/fail criteria

- **Unit:** 100% of argument-parsing and URL-construction branches covered; all P/H/C tests green.
- **Integration:** all E1–E8 and X1–X10 green (excluding optional X-variants explicitly marked manual).
- **Consumer:** `validate_duckdb.py` prints `VALIDATION PASSED` and per-file counts sum to total.
- **Production smoke:** return-read-only URL list is truly readable by DuckDB with `?download`.

### Coverage targets (guidance, not gates)

| Component | Target |
|-----------|--------|
| Arguments parsing | ≥ 95% line/branch |
| URL builders | 100% branch |
| ParquetBufferedWriter | ≥ 90% |
| HTTP client | ≥ 85% |
| Row conversion | ≥ 85% |

---

## 9. Traceability Matrix

| Success criterion (design) | Test(s) |
|-----------------------------|---------|
| No local file writes | E6 |
| Direct streaming via WebDAV | H4/H5, E2 |
| Parquet + Zstd | P12, C1–C7, E8 |
| Read-only URL generation | U2, E4 |
| DuckDB `read_parquet([...])` compatible | 5.2, 5.3, E7 |
| Handles medium-large datasets | 4.5, X8 |
| Error handling + cleanup of partials | X1–X10, F9 |

---

## 10. Open Items to Confirm with Implementer

1. **`--help` / `System.exit`** — needs refactor or the test is skipped (F1/A16).
2. **Empty-result contract** — zero files vs one empty file (E9/X9/C8).
3. **Row → Parquet approach** (design Option A vs B) — affects conversion test seams.
4. **`?download` suffix** — confirm with a real DESY URL during smoke test (F8).
5. **WebDAV DELETE support** on the target share — design lists this as an open question.
6. **Test server auth fidelity** — ensure the stub matches `Basic base64("token:")` exactly.
