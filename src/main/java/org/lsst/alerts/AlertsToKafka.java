package org.lsst.alerts;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import za.co.absa.abris.avro.functions$;
import za.co.absa.abris.config.AbrisConfig$;
import za.co.absa.abris.config.ToAvroConfig;

public final class AlertsToKafka {
    private static final String DEFAULT_WHERE = """
            diaSource.diaObjectId > 0
            and diaSource.reliability > 0.95
            and diaSource.visit >= 2026040100000
            and diaSource.visit < 2026043000000
            and diaSource.psfFlux > 0
            """.strip();
    private static final String DEFAULT_SCHEMA_REGISTRY =
            "https://rubin-alert-schemas.slac.stanford.edu/schema-registry";
    private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS =
            "kafka-kafka-bootstrap.lsst-kafka.svc.cluster.local:9092";
    private static final String DEFAULT_JAAS_CONFIG = "/kafka-user/sasl.jaas.config";
    private static final String DEFAULT_TOPIC = "lsst-ampel-dev-alerts";
    private static final Set<String> DEFAULT_SCHEMA_EXCLUDE = Set.of(
            "ssSource", "mpc_orbits", "cutoutTemplate", "cutoutScience", "cutoutDifference");

    private AlertsToKafka() {}

    public static void main(String[] args) throws Exception {
        Arguments options = Arguments.parse(args);
        SchemaVersion schemaVersion;
        Integer schemaId = options.schemaId;
        if (schemaId != null) {
            schemaVersion = schemaById(options.schemaRegistry, schemaId);
        } else {
            schemaVersion = latestSchema(options.schemaRegistry, options.subject);
            schemaId = schemaVersion.id();
        }

        ToAvroConfig avroConfig = AbrisConfig$.MODULE$
                .toConfluentAvro()
                .downloadSchemaById(schemaId)
                .usingSchemaRegistry(options.schemaRegistry);

        SparkSession spark = SparkSession.builder()
                .appName("AlertsToKafka")
                .getOrCreate();
        try {
            String query = buildQuery(options, schemaVersion.avroSchema());
            Dataset<Row> alerts = spark.sql(query);
            Dataset<Row> encodedAlerts = alerts.select(
                    functions.udf(
                            (Long diaSourceId) -> diaSourceId == null
                                ? null
                                : ByteBuffer.allocate(Long.BYTES)
                                    .putLong(diaSourceId)
                                    .array(),
                            DataTypes.BinaryType)
                        .apply(functions.col("value.diaSourceId"))
                        .alias("key"),
                    functions$.MODULE$
                            .to_avro(functions.col("value"), avroConfig)
                            .alias("value"));

            encodedAlerts.write()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", options.kafkaBootstrapServers)
                    .option("kafka.security.protocol", options.kafkaSecurityProtocol)
                    .option("kafka.sasl.mechanism", options.kafkaSecurityMechanism)
                    .option("kafka.sasl.jaas.config", Files.readString(options.jaasConfig))
                    .option("kafka.timeout.ms", options.kafkaTimeoutMs)
                    .option("topic", options.topic)
                    .save();
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

    /**
     * Builds a SQL query that constructs a struct matching the target Avro schema.
     * This ensures that only fields present in the target schema are selected,
     * avoiding serialization errors when the source table has additional fields.
     */
    private static String buildQuery(Arguments options, Schema avroSchema) {
        if (avroSchema == null || avroSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Target schema must be a RECORD type");
        }

        // Filter out excluded top-level fields
        List<Schema.Field> filteredFields = avroSchema.getFields().stream()
                .filter(field -> !options.schemaExclude.contains(field.name()))
                .collect(Collectors.toList());

        if (filteredFields.isEmpty()) {
            throw new IllegalArgumentException("Schema exclusions removed every schema field");
        }

        // Recursively build column expressions for each field
        List<String> columnExpressions = new ArrayList<>();
        for (Schema.Field field : filteredFields) {
            String expression = buildColumnExpression(field.schema(), field.name(), "");
            columnExpressions.add(expression);
        }

        StringBuilder query = new StringBuilder("""
                select
                  struct(
                    %s
                  ) as value
                  from lsst.alerts where %s""".formatted(
                String.join(",\n    ", columnExpressions),
                options.where));

        if (options.orderBy != null) {
            query.append(" order by ").append(options.orderBy);
        }
        if (options.limit != null && options.limit != 0) {
            query.append(" limit ").append(options.limit);
        }
        return query.toString();
    }

    /**
     * Checks if a schema represents a primitive type (including enums and fixed).
     * In Avro, primitive types are: STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, BYTES, NULL
     */
    private static boolean isPrimitiveType(Schema schema) {
        Schema.Type type = schema.getType();
        return type == Schema.Type.STRING ||
               type == Schema.Type.INT ||
               type == Schema.Type.LONG ||
               type == Schema.Type.FLOAT ||
               type == Schema.Type.DOUBLE ||
               type == Schema.Type.BOOLEAN ||
               type == Schema.Type.BYTES ||
               type == Schema.Type.NULL ||
               type == Schema.Type.ENUM ||
               type == Schema.Type.FIXED;
    }

    /**
     * Recursively builds a SQL column expression for a given Avro schema.
     * Handles primitive types, arrays, maps, unions, and nested records.
     *
     * @param schema The Avro schema for the field
     * @param fieldName The name of the field
     * @param prefix The dot-separated path to reach this field (used for qualified access)
     * @return SQL expression string for this field
     */
    private static String buildColumnExpression(Schema schema, String fieldName, String prefix) {
        String fullPath = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;

        // Resolve union types - get the actual type (non-null part)
        Schema resolvedSchema = resolveUnion(schema);
        Schema.Type type = resolvedSchema.getType();

        if (isPrimitiveType(resolvedSchema)) {
            // Primitive types: directly reference the column
            return fullPath;
        } else if (type == Schema.Type.RECORD) {
            // Nested record: build a struct() expression recursively
            List<String> nestedFields = new ArrayList<>();
            for (Schema.Field nestedField : resolvedSchema.getFields()) {
                String nestedExpr = buildColumnExpression(nestedField.schema(), nestedField.name(), fullPath);
                nestedFields.add(nestedExpr + " as " + nestedField.name());
            }
            return "struct(" + String.join(", ", nestedFields) + ") as " + fieldName;
        } else if (type == Schema.Type.ARRAY) {
            // Array type: reference directly (Iceberg/Spark will handle the array)
            return fullPath;
        } else if (type == Schema.Type.MAP) {
            // Map type: reference directly
            return fullPath;
        } else if (type == Schema.Type.UNION) {
            // Should have been resolved above, fallback to direct reference
            return fullPath;
        } else {
            throw new UnsupportedOperationException("Unsupported schema type: " + type);
        }
    }

    /**
     * Resolves a union type to its non-null member.
     * If the union contains both null and another type, returns the non-null type.
     * Otherwise returns the schema as-is.
     */
    private static Schema resolveUnion(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }

        List<Schema> types = schema.getTypes();
        if (types.size() == 2) {
            // Common case: ["null", SomeType]
            Schema first = types.get(0);
            Schema second = types.get(1);
            
            if (first.getType() == Schema.Type.NULL) {
                return second;
            }
            if (second.getType() == Schema.Type.NULL) {
                return first;
            }
        }
        
        // For more complex unions, just return the first non-null type
        for (Schema type : types) {
            if (type.getType() != Schema.Type.NULL) {
                return type;
            }
        }
        
        return schema;
    }

    private static SchemaVersion schemaById(String registryUrl, int schemaId) throws IOException, RestClientException {
        try (SchemaRegistryClient client = new CachedSchemaRegistryClient(registryUrl, 100)) {
            ParsedSchema schema = client.getSchemaById(schemaId);
            if (!(schema instanceof AvroSchema)) {
                throw new IOException("Schema ID " + schemaId + " is not an Avro schema");
            }
            AvroSchema avroSchema = (AvroSchema) schema;
            Schema parsedSchema = avroSchema.rawSchema();
            
            if (parsedSchema.getType() != Schema.Type.RECORD) {
                throw new IOException("Schema with ID " + schemaId + " is not a RECORD type");
            }
            
            return new SchemaVersion(schemaId, parsedSchema);
        }
    }

    private static SchemaVersion latestSchema(String registryUrl, String subject) throws IOException, RestClientException {
        try (SchemaRegistryClient client = new CachedSchemaRegistryClient(registryUrl, 100)) {
            int schemaId = client.getLatestSchemaMetadata(subject).getId();
            return schemaById(registryUrl, schemaId);
        }
    }

    private record SchemaVersion(int id, Schema avroSchema) {}

    static final class Arguments {
        private String where = DEFAULT_WHERE;
        private Integer limit;
        private String orderBy;
        private String schemaRegistry = DEFAULT_SCHEMA_REGISTRY;
        private String subject = "alert-packet";
        private Integer schemaId = null;
        private Set<String> schemaExclude = new LinkedHashSet<>(DEFAULT_SCHEMA_EXCLUDE);
        private String topic = DEFAULT_TOPIC;
        private String kafkaBootstrapServers = DEFAULT_KAFKA_BOOTSTRAP_SERVERS;
        private Path jaasConfig = Path.of(DEFAULT_JAAS_CONFIG);
        private String kafkaSecurityProtocol = "SASL_PLAINTEXT";
        private String kafkaSecurityMechanism = "SCRAM-SHA-512";
        private String kafkaTimeoutMs = "3000";
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
                    case "--schema-registry" -> options.schemaRegistry = value;
                    case "--subject" -> {
                        if (options.schemaId != null) {
                            throw new IllegalArgumentException("--subject and --schema-id are mutually exclusive");
                        }
                        options.subject = value;
                    }
                    case "--schema-id" -> {
                        if (!options.subject.equals("alert-packet")) {
                            throw new IllegalArgumentException("--subject and --schema-id are mutually exclusive");
                        }
                        options.schemaId = value.isEmpty() ? null : Integer.parseInt(value);
                    }
                    case "--topic" -> options.topic = value;
                    case "--kafka-bootstrap-servers" -> options.kafkaBootstrapServers = value;
                    case "--kafka-sasl-jaas-config" -> options.jaasConfig = Path.of(value);
                    case "--kafka-security-protocol" -> options.kafkaSecurityProtocol = value;
                    case "--kafka-security-mechanism" -> options.kafkaSecurityMechanism = value;
                    case "--kafka-timeout-ms" -> options.kafkaTimeoutMs = value;
                    case "--wait" -> options.waitSeconds = Integer.parseInt(value);
                    default -> throw new IllegalArgumentException("Unknown option: " + option);
                }
            }
            return options;
        }

        private static void printUsage() {
            System.out.println("""
                    Query alerts from Iceberg and write them to Kafka in Avro format.
                    Usage: spark-submit --class org.lsst.alerts.AlertsToKafka alerts-to-kafka.jar [options]
                      --where <sql-predicate>
                      --limit <count>
                      --order-by <sql-expression>
                      --schema-registry <url>
                      --subject <schema-registry-subject>  (mutually exclusive with --schema-id)
                      --schema-id <id>                       (mutually exclusive with --subject)
                      --schema-exclude <field> [<field> ...]
                      --topic <name>
                      --kafka-bootstrap-servers <servers>
                      --kafka-sasl-jaas-config <file>
                      --kafka-security-protocol <protocol>
                      --kafka-security-mechanism <mechanism>
                      --kafka-timeout-ms <milliseconds>
                      --wait <seconds>
                    """);
        }
    }
}
