package org.lsst.alerts;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;

/**
 * Query alerts from Iceberg, stream the results as Parquet files directly to a
 * Nextcloud (DESY Sync &amp; Share) share via WebDAV, and print read-only URLs
 * for consumers.
 *
 * <p>No data is written to local disk: each Spark partition is streamed through
 * an HTTP {@code PUT} request wrapped in a custom {@link OutputFile}.
 */
public final class AlertsToNextcloud {
    private static final String DEFAULT_WHERE = """
            diaSource.diaObjectId > 0
            and diaSource.reliability > 0.95
            and diaSource.visit >= 2026040100000
            and diaSource.visit < 2026043000000
            and diaSource.psfFlux > 0
            """.strip();
    private static final Set<String> DEFAULT_SCHEMA_EXCLUDE = Set.of(
            "ssSource", "mpc_orbits", "cutoutTemplate", "cutoutScience", "cutoutDifference");

    /**
     * Target row-group size for the streaming Parquet writer. Row groups are
     * buffered in executor memory before being compressed and written, so this
     * bounds the per-partition memory footprint.
     */
    private static final long ROW_GROUP_SIZE = 8L * 1024 * 1024;

    /** Target page size for the streaming Parquet writer. */
    private static final int PAGE_SIZE = 1024 * 1024;

    /** Size of the in-memory buffer connecting the Parquet writer to the HTTP sender. */
    private static final int PIPE_BUFFER_SIZE = 1024 * 1024;

    private AlertsToNextcloud() {}

    public static void main(String[] args) throws Exception {
        Arguments options = Arguments.parse(args);

        SparkSession spark = SparkSession.builder()
                .appName("AlertsToNextcloud")
                .getOrCreate();
        try {
            Dataset<Row> alerts = spark.sql(buildQuery(options));
            Dataset<Row> projected = applySchemaExclusions(alerts, options.schemaExclude);

            MessageType parquetSchema = convertToParquetSchema(projected.schema());
            List<String> downloadUrls = uploadPartitions(projected, parquetSchema, options);

            for (String url : downloadUrls) {
                System.out.println(url);
            }
        } finally {
            try {
                if (options.waitSeconds > 0) {
                    System.out.printf(
                            "Write finished; keeping driver alive for %d seconds for inspection%n",
                            options.waitSeconds);
                    TimeUnit.SECONDS.sleep(options.waitSeconds);
                }
            } finally {
                spark.stop();
            }
        }
    }

    private static String buildQuery(Arguments options) {
        StringBuilder query = new StringBuilder("""
                select
                  *
                  from lsst.alerts where %s""".formatted(options.where));

        if (options.orderBy != null) {
            query.append(" order by ").append(options.orderBy);
        }
        if (options.limit != null && options.limit != 0) {
            query.append(" limit ").append(options.limit);
        }
        return query.toString();
    }

    /**
     * Drops the fields listed in {@code schemaExclude} from the query result.
     * The alerts table is queried as raw columns, so exclusions are applied on
     * the resulting {@link Dataset} rather than in the SQL projection.
     */
    private static Dataset<Row> applySchemaExclusions(Dataset<Row> alerts, Set<String> schemaExclude) {
        if (schemaExclude.isEmpty()) {
            return alerts;
        }
        List<String> columns = Arrays.stream(alerts.columns())
                .filter(column -> !schemaExclude.contains(column))
                .toList();
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("Schema exclusions removed every schema field");
        }
        return alerts.selectExpr(columns.toArray(String[]::new));
    }

    /**
     * Streams every Spark partition of {@code alerts} to Nextcloud as a single
     * Zstd-compressed Parquet object, and returns the read-only download URL of
     * each uploaded object.
     *
     * <p>Uploads run on the executors via {@code mapPartitions}; only the small
     * list of download URLs is collected back to the driver, so the driver never
     * holds alert data in memory.
     */
    private static List<String> uploadPartitions(
            Dataset<Row> alerts, MessageType parquetSchema, Arguments options) {
        // Broadcast the Spark schema (which IS serializable) instead of the Parquet schema
        // We'll convert to Parquet schema on each worker
        StructType sparkSchema = alerts.schema();
        
        JavaRDD<String> urlRDD = alerts.rdd()
                .toJavaRDD()
                .mapPartitions(it -> {
                    // Recreate Parquet schema from Spark schema on each worker
                    MessageType workerParquetSchema = convertToParquetSchema(sparkSchema);
                    return uploadPartition(it, workerParquetSchema, options);
                });
        return urlRDD.collect();
    }

    private static Iterator<String> uploadPartition(
            Iterator<Row> iterator, MessageType parquetSchema, Arguments options) {
        int partitionId = TaskContext.get().partitionId();
        String filename = options.filenamePrefix + "_part-" + partitionId + ".parquet";
        String webDavUrl = buildWebDavUrl(options.nextcloudBaseUrl, options.nextcloudUploadToken, filename);
        String downloadUrl = buildDownloadUrl(
                options.nextcloudBaseUrl, options.nextcloudDownloadToken, filename);

        HttpClient client = createHttpClient();
        try {
            uploadPartitionAsParquet(
                    client, iterator, parquetSchema, webDavUrl, options.nextcloudUploadToken);
            return Collections.singletonList(downloadUrl).iterator();
        } catch (Exception e) {
            cleanupPartialUpload(client, webDavUrl, options.nextcloudUploadToken);
            throw new RuntimeException("Failed uploading partition " + partitionId, e);
        } finally {
            // HttpClient is not AutoCloseable on Java 17; it needs no explicit close.
        }
    }

    /**
     * Writes every row of one partition as a Zstd Parquet object and PUTs it to
     * {@code webDavUrl}, without ever materialising the file on local disk.
     *
     * <p>The Parquet bytes are produced lazily and consumed by an HTTP
     * {@code BodyPublisher} that streams them straight onto the wire.
     */
    static void uploadPartitionAsParquet(
            HttpClient client,
            Iterator<Row> rows,
            MessageType parquetSchema,
            String webDavUrl,
            String uploadToken)
            throws IOException, InterruptedException {
        // A piped stream connects the Parquet writer (producing bytes on a worker
        // thread) to the HTTP request body (consumed lazily by the sender).
        PipedInputStream pipedIn = new PipedInputStream(PIPE_BUFFER_SIZE);
        PipedOutputStream pipedOut = new PipedOutputStream(pipedIn);

        HttpRequest request = buildPutRequest(
                client, webDavUrl, uploadToken, HttpRequest.BodyPublishers.ofInputStream(() -> pipedIn));

        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread writer = new Thread(
                () -> {
                    try {
                        writeParquet(rows, parquetSchema, new ParquetBufferedWriter(pipedOut));
                    } catch (Throwable t) {
                        writerFailure.set(t);
                    } finally {
                        // Closing pipedOut signals EOF (or failure) to the sender.
                        closeQuietly(pipedOut);
                    }
                },
                "parquet-writer");
        writer.setDaemon(true);
        writer.start();

        HttpResponse<Void> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (IOException | RuntimeException e) {
            // Prefer the writer's failure (if any) over the resulting broken-pipe
            // error observed on the sending side.
            closeQuietly(pipedIn);
            joinWriter(writer);
            Throwable cause = writerFailure.get();
            if (cause != null) {
                throw new IOException("Failed writing Parquet for " + webDavUrl, cause);
            }
            throw e;
        }
        joinWriter(writer);
        Throwable cause = writerFailure.get();
        if (cause != null) {
            throw new IOException("Failed writing Parquet for " + webDavUrl, cause);
        }

        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            throw new IOException("Nextcloud WebDAV PUT for " + webDavUrl + " returned HTTP " + status);
        }
    }

    /** Creates a Parquet writer over {@code outputFile} and writes all {@code rows}. */
    static void writeParquet(Iterator<Row> rows, MessageType parquetSchema, OutputFile outputFile)
            throws IOException {
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withType(parquetSchema)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withRowGroupSize(ROW_GROUP_SIZE)
                .withPageSize(PAGE_SIZE)
                .build()) {
            while (rows.hasNext()) {
                writer.write(convertRowToGroup(rows.next(), parquetSchema));
            }
        }
    }

    private static void joinWriter(Thread writer) throws IOException {
        try {
            writer.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while writing Parquet to Nextcloud", e);
        }
        if (writer.isAlive()) {
            throw new IOException("Parquet writer did not terminate");
        }
    }

    /**
     * Best-effort removal of a partially uploaded object. Cleanup failures are
     * logged and swallowed so the original upload exception is not masked.
     */
    static void cleanupPartialUpload(HttpClient client, String webDavUrl, String uploadToken) {
        try {
            HttpRequest request = buildDeleteRequest(client, webDavUrl, uploadToken);
            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            int status = response.statusCode();
            if (status < 200 || status >= 300) {
                System.err.println(
                        "Warning: cleanup DELETE for " + webDavUrl + " returned HTTP " + status);
            }
        } catch (Exception e) {
            System.err.println("Warning: failed to clean up partial upload " + webDavUrl + ": " + e);
        }
    }

    private static void closeQuietly(java.io.Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException ignored) {
            // Nothing useful to do at this point.
        }
    }

    /**
     * Converts a Spark {@link StructType} to a Parquet {@link MessageType}.
     *
     * <p>Primitive Spark types map directly onto Parquet primitives. Nested
     * structs become nested Parquet groups. Nullability is carried across so
     * optional columns round-trip as nullable.
     */
    static MessageType convertToParquetSchema(StructType sparkSchema) {
        List<Type> fields = new ArrayList<>();
        for (StructField field : sparkSchema.fields()) {
            fields.add(convertField(field));
        }
        return new MessageType("alert", fields);
    }

    private static Type convertField(StructField field) {
        Type.Repetition repetition =
                field.nullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
        return convertType(field.dataType(), repetition, field.name());
    }

    private static Type convertType(DataType dataType, Type.Repetition repetition, String name) {
        if (dataType instanceof ByteType || dataType instanceof ShortType || dataType instanceof IntegerType) {
            return Types.primitive(PrimitiveTypeName.INT32, repetition).named(name);
        }
        if (dataType instanceof LongType) {
            return Types.primitive(PrimitiveTypeName.INT64, repetition).named(name);
        }
        if (dataType instanceof FloatType) {
            return Types.primitive(PrimitiveTypeName.FLOAT, repetition).named(name);
        }
        if (dataType instanceof DoubleType) {
            return Types.primitive(PrimitiveTypeName.DOUBLE, repetition).named(name);
        }
        if (dataType instanceof BooleanType) {
            return Types.primitive(PrimitiveTypeName.BOOLEAN, repetition).named(name);
        }
        if (dataType instanceof StringType) {
            return Types.primitive(PrimitiveTypeName.BINARY, repetition)
                    .as(LogicalTypeAnnotation.stringType())
                    .named(name);
        }
        if (dataType instanceof BinaryType) {
            return Types.primitive(PrimitiveTypeName.BINARY, repetition).named(name);
        }
        if (dataType instanceof DateType) {
            return Types.primitive(PrimitiveTypeName.INT32, repetition)
                    .as(LogicalTypeAnnotation.dateType())
                    .named(name);
        }
        if (dataType instanceof TimestampType || dataType instanceof TimestampNTZType) {
            boolean adjusted = dataType instanceof TimestampType;
            return Types.primitive(PrimitiveTypeName.INT64, repetition)
                    .as(LogicalTypeAnnotation.timestampType(
                            adjusted, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .named(name);
        }
        if (dataType instanceof DecimalType decimal) {
            return Types.primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
                    .as(LogicalTypeAnnotation.decimalType(decimal.scale(), decimal.precision()))
                    .length(decimalMaxBytes(decimal))
                    .named(name);
        }
        if (dataType instanceof StructType struct) {
            return convertStructType(struct, repetition, name);
        }
        if (dataType instanceof ArrayType array) {
            return convertArrayType(array, repetition, name);
        }
        if (dataType instanceof MapType map) {
            return convertMapType(map, repetition, name);
        }
        if (dataType instanceof NullType) {
            // Parquet has no void type; Spark itself writes NullType as a nullable
            // int32 whose values are always absent. Guard the repetition so the
            // resulting column is always optional regardless of the declared field.
            return Types.optional(PrimitiveTypeName.INT32).named(name);
        }
        throw new UnsupportedOperationException(
                "Unsupported Spark type for Parquet export: " + dataType.typeName());
    }

    private static Type convertStructType(StructType struct, Type.Repetition repetition, String name) {
        List<Type> fields = new ArrayList<>();
        for (StructField field : struct.fields()) {
            fields.add(convertField(field));
        }
        return Types.buildGroup(repetition).addFields(fields.toArray(Type[]::new)).named(name);
    }

    /**
     * Maps a Spark {@link ArrayType} onto the Parquet three-level {@code LIST}
     * layout: an optional/repeated group {@code <name>} containing a repeated
     * group {@code list} that in turn holds the element field. This is the
     * canonical representation understood by Spark, DuckDB and pyarrow.
     */
    private static Type convertArrayType(ArrayType array, Type.Repetition repetition, String name) {
        Type element = convertType(
                array.elementType(),
                array.containsNull() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED,
                "element");
        return Types.buildGroup(repetition)
                .addField(Types.buildGroup(Type.Repetition.REPEATED)
                        .addField(element)
                        .named("list"))
                .as(LogicalTypeAnnotation.listType())
                .named(name);
    }

    /**
     * Maps a Spark {@link MapType} onto the Parquet canonical map layout: a
     * repeated group {@code key_value} with a required {@code key} field and a
     * value field whose nullability follows {@link MapType#valueContainsNull()}.
     */
    private static Type convertMapType(MapType map, Type.Repetition repetition, String name) {
        Type key = convertType(map.keyType(), Type.Repetition.REQUIRED, "key");
        Type value = convertType(
                map.valueType(),
                map.valueContainsNull() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED,
                "value");
        return Types.buildGroup(repetition)
                .addField(Types.buildGroup(Type.Repetition.REPEATED)
                        .addField(key)
                        .addField(value)
                        .named("key_value"))
                .as(LogicalTypeAnnotation.mapType())
                .named(name);
    }

    private static int decimalMaxBytes(DecimalType decimal) {
        // Minimal two's-complement byte width that fits the precision.
        return (int) Math.ceil(
                (Math.log(Math.pow(10, decimal.precision()) - 1) / Math.log(2) + 1) / 8);
    }

    /**
     * Converts a Spark {@link Row} into a Parquet {@link Group} matching
     * {@code schema}. {@code null} values are left unset, which is how Parquet
     * represents optional fields.
     */
    static Group convertRowToGroup(Row row, MessageType schema) {
        Group group = new SimpleGroup(schema);
        appendRow(group, row, schema, 0);
        return group;
    }

    private static void appendRow(Group group, Row row, GroupType groupType, int offset) {
        for (int i = 0; i < groupType.getFieldCount(); i++) {
            if (row.isNullAt(offset + i)) {
                continue;
            }
            appendValue(group, i, groupType.getType(i), row.get(offset + i));
        }
    }

    private static void appendValue(Group group, int fieldIndex, Type fieldType, Object value) {
        if (fieldType.isPrimitive()) {
            appendPrimitive(group, fieldIndex, fieldType.asPrimitiveType(), value);
            return;
        }
        LogicalTypeAnnotation logicalType = fieldType.getLogicalTypeAnnotation();
        if (logicalType instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
            appendList(group.addGroup(fieldIndex), value);
            return;
        }
        if (logicalType instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
            appendMap(group.addGroup(fieldIndex), value);
            return;
        }
        appendRow(group.addGroup(fieldIndex), (Row) value, fieldType.asGroupType(), 0);
    }

    /**
     * Appends the elements of a Spark array value to the repeated {@code list}
     * element field of a Parquet LIST group.
     */
    private static void appendList(Group listGroup, Object value) {
        GroupType listType = listGroup.getType();
        Type elementType = listType.getType("list").asGroupType().getType(0);
        for (Object element : asIterable(value)) {
            Group repeated = listGroup.addGroup("list");
            if (element == null) {
                continue;
            }
            appendValue(repeated, 0, elementType, element);
        }
    }

    /**
     * Appends the entries of a Spark map value to the repeated {@code key_value}
     * group of a Parquet MAP group.
     */
    private static void appendMap(Group mapGroup, Object value) {
        GroupType mapType = mapGroup.getType();
        GroupType keyValueType = mapType.getType("key_value").asGroupType();
        Type keyType = keyValueType.getType(0);
        Type valueType = keyValueType.getType(1);
        for (Map.Entry<?, ?> entry : asEntryIterable(value)) {
            Group keyValue = mapGroup.addGroup("key_value");
            appendValue(keyValue, 0, keyType, entry.getKey());
            if (entry.getValue() != null) {
                appendValue(keyValue, 1, valueType, entry.getValue());
            }
        }
    }

    /** Adapts the several runtime representations Spark uses for arrays to an {@link Iterable}. */
    private static Iterable<?> asIterable(Object value) {
        if (value instanceof Iterable<?> iterable) {
            return iterable;
        }
        if (value instanceof Object[] array) {
            return Arrays.asList(array);
        }
        if (value instanceof scala.collection.Seq<?> seq) {
            return scala.collection.JavaConverters.seqAsJavaList(seq);
        }
        if (value instanceof scala.collection.Iterable<?> iterable) {
            return scala.collection.JavaConverters.asJavaIterable(iterable);
        }
        throw new IllegalArgumentException("Cannot convert array value of type " + value.getClass());
    }

    /** Adapts the several runtime representations Spark uses for maps to entries. */
    private static Iterable<? extends Map.Entry<?, ?>> asEntryIterable(Object value) {
        if (value instanceof Map<?, ?> map) {
            return map.entrySet();
        }
        if (value instanceof scala.collection.Map<?, ?> map) {
            return scala.collection.JavaConverters.mapAsJavaMap(map).entrySet();
        }
        throw new IllegalArgumentException("Cannot convert map value of type " + value.getClass());
    }

    private static void appendPrimitive(
            Group group, int fieldIndex, org.apache.parquet.schema.PrimitiveType fieldType, Object value) {
        PrimitiveTypeName typeName = fieldType.getPrimitiveTypeName();
        switch (typeName) {
            case INT32 -> {
                if (value instanceof java.sql.Date date) {
                    group.add(fieldIndex, (int) date.toLocalDate().toEpochDay());
                } else if (value instanceof LocalDate date) {
                    group.add(fieldIndex, (int) date.toEpochDay());
                } else if (value instanceof Number number) {
                    group.add(fieldIndex, number.intValue());
                } else {
                    group.add(fieldIndex, (int) value);
                }
            }
            case INT64 -> group.add(fieldIndex, toEpochMicros(value));
            case FLOAT -> group.add(fieldIndex, ((Number) value).floatValue());
            case DOUBLE -> group.add(fieldIndex, ((Number) value).doubleValue());
            case BOOLEAN -> group.add(fieldIndex, (boolean) value);
            case BINARY -> group.add(fieldIndex, toBinary(value));
            case FIXED_LEN_BYTE_ARRAY -> group.add(fieldIndex, toFixedBinary(value, fieldType.getTypeLength()));
            default -> throw new UnsupportedOperationException(
                    "Unsupported Parquet primitive type: " + typeName);
        }
    }

    /**
     * Converts the several temporal representations Spark uses for timestamps
     * into microseconds since the Unix epoch (the Parquet TIMESTAMP unit).
     */
    private static long toEpochMicros(Object value) {
        if (value instanceof java.sql.Timestamp timestamp) {
            return timestamp.getTime() * 1000L;
        }
        if (value instanceof java.time.Instant instant) {
            return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
        }
        if (value instanceof java.time.LocalDateTime localDateTime) {
            return localDateTime.toInstant(java.time.ZoneOffset.UTC).getEpochSecond() * 1_000_000L
                    + localDateTime.getNano() / 1_000L;
        }
        if (value instanceof java.time.OffsetDateTime offsetDateTime) {
            java.time.Instant instant = offsetDateTime.toInstant();
            return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
        }
        return ((Number) value).longValue();
    }

    private static Binary toBinary(Object value) {
        if (value instanceof Binary binary) {
            return binary;
        }
        if (value instanceof byte[] bytes) {
            return Binary.fromConstantByteArray(bytes);
        }
        if (value instanceof BigDecimal decimal) {
            // FIXED_LEN_BYTE_ARRAY decimals are stored big-endian two's complement.
            return Binary.fromConstantByteArray(decimal.unscaledValue().toByteArray());
        }
        return Binary.fromString(value.toString());
    }

    /**
     * Encodes a value into a FIXED_LEN_BYTE_ARRAY of exactly {@code length}
     * bytes, sign-extending decimals into the full column width.
     */
    private static Binary toFixedBinary(Object value, int length) {
        byte[] bytes;
        if (value instanceof BigDecimal decimal) {
            bytes = decimal.unscaledValue().toByteArray();
        } else if (value instanceof byte[] raw) {
            bytes = raw;
        } else if (value instanceof Binary binary) {
            bytes = binary.getBytes();
        } else {
            bytes = value.toString().getBytes(StandardCharsets.UTF_8);
        }
        if (bytes.length == length) {
            return Binary.fromConstantByteArray(bytes);
        }
        if (bytes.length > length) {
            throw new IllegalArgumentException(
                    "Value of " + bytes.length + " bytes does not fit in fixed length " + length);
        }
        // Pad on the left, preserving the sign bit (two's complement).
        byte pad = (bytes.length > 0 && bytes[0] < 0) ? (byte) 0xFF : (byte) 0x00;
        byte[] padded = new byte[length];
        Arrays.fill(padded, 0, length - bytes.length, pad);
        System.arraycopy(bytes, 0, padded, length - bytes.length, bytes.length);
        return Binary.fromConstantByteArray(padded);
    }

    /**
     * Builds the WebDAV {@code PUT} URL for uploading a file to a Nextcloud
     * public share.
     *
     * @param baseUrl Nextcloud instance base URL (e.g. {@code https://syncandshare.desy.de})
     * @param token read-write share token
     * @param filename name of the file to upload
     * @return the full WebDAV URL
     */
    static String buildWebDavUrl(String baseUrl, String token, String filename) {
        return normalizeBaseUrl(baseUrl)
                + "/public.php/dav/files/"
                + token
                + "/"
                + encodePathSegment(filename);
    }

    /**
     * Builds the read-only download URL for a file in a Nextcloud public share.
     *
     * @param baseUrl Nextcloud instance base URL (e.g. {@code https://syncandshare.desy.de})
     * @param token read-only share token
     * @param filename name of the file to download
     * @return the full download URL
     */
    static String buildDownloadUrl(String baseUrl, String token, String filename) {
        return normalizeBaseUrl(baseUrl)
                + "/index.php/s/"
                + token
                + "/download?path="
                + encodePathSegment("/" + filename);
    }

    private static String normalizeBaseUrl(String baseUrl) {
        return baseUrl.replaceAll("/+$", "");
    }

    private static String encodePathSegment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /**
     * Creates the shared {@link HttpClient} used for Nextcloud WebDAV
     * interactions. Connections are HTTP/1.1 to match the DESY WebDAV endpoint.
     */
    static HttpClient createHttpClient() {
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }

    /**
     * Builds an HTTP {@code PUT} request for uploading {@code body} to the
     * given WebDAV URL using the supplied upload token.
     *
     * @param client the HTTP client to use
     * @param url the WebDAV upload URL
     * @param token the read-write share token used as the Basic auth username
     * @param body publisher providing the Parquet bytes
     * @return the configured request
     */
    static HttpRequest buildPutRequest(
            HttpClient client, String url, String token, HttpRequest.BodyPublisher body) {
        return HttpRequest.newBuilder(URI.create(url))
                .header("Authorization", basicAuth(token, ""))
                .header("X-Requested-With", "XMLHttpRequest")
                .timeout(Duration.ofMinutes(30))
                .PUT(body)
                .build();
    }

    /**
     * Builds an HTTP {@code DELETE} request used to remove a partially uploaded
     * file after a failed transfer.
     *
     * @param client the HTTP client to use
     * @param url the WebDAV URL of the file to delete
     * @param token the read-write share token used as the Basic auth username
     * @return the configured request
     */
    static HttpRequest buildDeleteRequest(HttpClient client, String url, String token) {
        return HttpRequest.newBuilder(URI.create(url))
                .header("Authorization", basicAuth(token, ""))
                .header("X-Requested-With", "XMLHttpRequest")
                .timeout(Duration.ofMinutes(5))
                .DELETE()
                .build();
    }

    private static String basicAuth(String username, String password) {
        String credentials = username + ":" + password;
        return "Basic " + Base64.getEncoder()
                .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * An {@link OutputFile} that writes Parquet bytes to an arbitrary
     * non-seekable {@link OutputStream}, tracking the stream position manually.
     *
     * <p>This allows streaming a Parquet file directly to an HTTP connection
     * without buffering the whole file locally.
     */
    static final class ParquetBufferedWriter implements OutputFile {
        private final OutputStream output;
        private final long blockSizeHint;

        ParquetBufferedWriter(OutputStream output) {
            this(output, 128L * 1024 * 1024);
        }

        ParquetBufferedWriter(OutputStream output, long blockSizeHint) {
            this.output = output;
            this.blockSizeHint = blockSizeHint;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return positionOutputStream();
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return positionOutputStream();
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return blockSizeHint;
        }

        private PositionOutputStream positionOutputStream() {
            BufferedOutputStream buffered = new BufferedOutputStream(output);
            return new PositionOutputStream() {
                private long pos = 0;

                @Override
                public long getPos() {
                    return pos;
                }

                @Override
                public void write(int b) throws IOException {
                    buffered.write(b);
                    pos++;
                }

                @Override
                public void write(byte[] b) throws IOException {
                    buffered.write(b);
                    pos += b.length;
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    buffered.write(b, off, len);
                    pos += len;
                }

                @Override
                public void flush() throws IOException {
                    buffered.flush();
                }

                @Override
                public void close() throws IOException {
                    buffered.close();
                }
            };
        }
    }

    private static final class Arguments implements Serializable {
        private String where = DEFAULT_WHERE;
        private Integer limit;
        private String orderBy;
        private Set<String> schemaExclude = new LinkedHashSet<>(DEFAULT_SCHEMA_EXCLUDE);
        private String nextcloudBaseUrl;
        private String nextcloudUploadToken;
        private String nextcloudDownloadToken;
        private String filenamePrefix;
        private int waitSeconds;

        private static Arguments parse(String[] args) {
            Arguments options = new Arguments();
            for (int index = 0; index < args.length; index++) {
                String option = args[index];
                if (option.equals("--help")) {
                    printUsage();
                    System.exit(0);
                }
                if (option.equals("--schema-exclude")) {
                    options.schemaExclude = new LinkedHashSet<>();
                    int valueCount = 0;
                    while (index + 1 < args.length && !args[index + 1].startsWith("--")) {
                        options.schemaExclude.add(args[++index]);
                        valueCount++;
                    }
                    if (valueCount == 0) {
                        throw new IllegalArgumentException("Missing values for --schema-exclude");
                    }
                    continue;
                }

                if (index + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for " + option);
                }
                String value = args[++index];
                switch (option) {
                    case "--where" -> options.where = value;
                    case "--limit" -> options.limit = Integer.parseInt(value);
                    case "--order-by" -> options.orderBy = value;
                    case "--nextcloud-base-url" -> options.nextcloudBaseUrl = value;
                    case "--nextcloud-upload-token" -> options.nextcloudUploadToken = value;
                    case "--nextcloud-download-token" -> options.nextcloudDownloadToken = value;
                    case "--filename-prefix" -> options.filenamePrefix = value;
                    case "--wait" -> options.waitSeconds = Integer.parseInt(value);
                    default -> throw new IllegalArgumentException("Unknown option: " + option);
                }
            }
            options.validate();
            return options;
        }

        private void validate() {
            require(nextcloudBaseUrl, "--nextcloud-base-url");
            require(nextcloudUploadToken, "--nextcloud-upload-token");
            require(nextcloudDownloadToken, "--nextcloud-download-token");
            require(filenamePrefix, "--filename-prefix");
        }

        private static void require(String value, String option) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Missing required option: " + option);
            }
        }

        private static void printUsage() {
            System.out.println("""
                    Query alerts from Iceberg and stream them to a Nextcloud share as Parquet.
                    Usage: spark-submit --class org.lsst.alerts.AlertsToNextcloud alerts-to-nextcloud.jar [options]
                      --where <sql-predicate>
                      --limit <count>
                      --order-by <sql-expression>
                      --schema-exclude <field> [<field> ...]
                      --nextcloud-base-url <url>
                      --nextcloud-upload-token <token>
                      --nextcloud-download-token <token>
                      --filename-prefix <prefix>
                      --wait <seconds>
                    """);
        }
    }
}
