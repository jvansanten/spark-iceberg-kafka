package org.lsst.alerts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
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
        SchemaVersion schemaVersion = latestSchema(options.schemaRegistry, options.subject);

        ToAvroConfig avroConfig = AbrisConfig$.MODULE$
                .toConfluentAvro()
                .downloadSchemaById(schemaVersion.id())
                .usingSchemaRegistry(options.schemaRegistry);

        SparkSession spark = SparkSession.builder()
                .appName("AlertsToKafka")
                .getOrCreate();
        try {
            String query = buildQuery(options, schemaVersion.fields());
            Dataset<Row> alerts = spark.sql(query);
            Dataset<Row> encodedAlerts = alerts.select(
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

    private static String buildQuery(Arguments options, List<String> schemaFields) {
        List<String> columns = schemaFields.stream()
                .filter(field -> !options.schemaExclude.contains(field))
                .toList();
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("Schema exclusions removed every schema field");
        }

        StringBuilder query = new StringBuilder("""
                select
                  struct(
                    %s
                  ) as value
                  from lsst.alerts where %s""".formatted(
                String.join(",\n    ", columns), options.where));

        if (options.orderBy != null) {
            query.append(" order by ").append(options.orderBy);
        }
        if (options.limit != null && options.limit != 0) {
            query.append(" limit ").append(options.limit);
        }
        return query.toString();
    }

    private static SchemaVersion latestSchema(String registryUrl, String subject)
            throws IOException, InterruptedException {
        String normalizedRegistryUrl = registryUrl.replaceAll("/+$", "");
        String encodedSubject = URLEncoder.encode(subject, StandardCharsets.UTF_8)
                .replace("+", "%20");
        URI uri = URI.create(normalizedRegistryUrl + "/subjects/" + encodedSubject + "/versions/latest");
        HttpRequest request = HttpRequest.newBuilder(uri).GET().build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Schema Registry returned HTTP " + response.statusCode()
                    + " for subject " + subject + ": " + response.body());
        }

        JsonNode latest = new ObjectMapper().readTree(response.body());
        JsonNode id = latest.get("id");
        JsonNode schemaText = latest.get("schema");
        if (id == null || !id.canConvertToInt() || schemaText == null || !schemaText.isTextual()) {
            throw new IOException("Latest schema response is missing an integer id or schema for subject "
                    + subject);
        }

        JsonNode schema = new ObjectMapper().readTree(schemaText.textValue());
        JsonNode fieldsNode = schema.get("fields");
        if (fieldsNode == null || !fieldsNode.isArray()) {
            throw new IOException("Latest schema has no fields array for subject " + subject);
        }

        List<String> fields = new ArrayList<>();
        for (JsonNode field : fieldsNode) {
            JsonNode name = field.get("name");
            if (name != null && name.isTextual()) {
                fields.add(name.textValue());
            }
        }
        if (fields.isEmpty()) {
            throw new IOException("Latest schema has no named fields for subject " + subject);
        }
        return new SchemaVersion(id.intValue(), List.copyOf(fields));
    }

    private record SchemaVersion(int id, List<String> fields) {}

    private static final class Arguments {
        private String where = DEFAULT_WHERE;
        private Integer limit;
        private String orderBy;
        private String schemaRegistry = DEFAULT_SCHEMA_REGISTRY;
        private String subject = "alert-packet";
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
                    case "--subject" -> options.subject = value;
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
                      --subject <schema-registry-subject>
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