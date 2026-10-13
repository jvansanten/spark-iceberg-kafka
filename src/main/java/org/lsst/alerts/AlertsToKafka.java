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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import za.co.absa.abris.avro.functions$;
import za.co.absa.abris.config.AbrisConfig$;
import za.co.absa.abris.config.ToAvroConfig;

public final class AlertsToKafka {
    private static final String DEFAULT_WHERE =
            "diaSource.diaObjectId > 0 and diaSource.reliability > 0.95 "
                    + "and diaSource.visit > 2026040100000 and diaSource.psfFlux > 0";
    private static final String DEFAULT_SCHEMA_REGISTRY =
            "https://rubin-alert-schemas.slac.stanford.edu/schema-registry";
    private static final String KAFKA_BOOTSTRAP_SERVERS =
            "kafka-kafka-bootstrap.lsst-kafka.svc.cluster.local:9092";
    private static final String KAFKA_TOPIC = "lsst-ampel-dev-alerts";

    private AlertsToKafka() {}

    public static void main(String[] args) throws Exception {
        Arguments options = Arguments.parse(args);
        String kafkaUsername = requiredEnvironmentVariable("KAFKA_USERNAME");
        String kafkaPassword = requiredEnvironmentVariable("KAFKA_PASSWORD");
        int schemaId = latestSchemaId(options.schemaRegistry, options.subject);

        ToAvroConfig avroConfig = AbrisConfig$.MODULE$
                .toConfluentAvro()
                .downloadSchemaById(schemaId)
                .usingSchemaRegistry(options.schemaRegistry);

        SparkSession spark = SparkSession.builder()
                .appName("SparkTest")
                .getOrCreate();
        try {
            Dataset<Row> alerts = spark.sql(buildQuery(options));
            Dataset<Row> encodedAlerts = alerts.select(
                    functions$.MODULE$
                            .to_avro(functions.col("value"), avroConfig)
                            .alias("value"));

            encodedAlerts.write()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                    .option("kafka.security.protocol", "SASL_PLAINTEXT")
                    .option("kafka.sasl.username", kafkaUsername)
                    .option("kafka.sasl.password", kafkaPassword)
                    .option("topic", KAFKA_TOPIC)
                    .save();
        } finally {
            spark.stop();
        }
    }

    private static String buildQuery(Arguments options) {
        StringBuilder query = new StringBuilder("""
                select
                  struct(
                    diaSourceId,
                    target_name,
                    observation_reason,
                    diaSource,
                    diaObject,
                    prvDiaSources,
                    prvDiaForcedSources
                  ) as value
                  from lsst.alerts where %s""".formatted(options.where));

        if (options.orderBy != null) {
            query.append(" order by ").append(options.orderBy);
        }
        if (options.limit != null && options.limit != 0) {
            query.append(" limit ").append(options.limit);
        }
        return query.toString();
    }

    private static int latestSchemaId(String registryUrl, String subject)
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

        JsonNode schema = new ObjectMapper().readTree(response.body());
        JsonNode id = schema.get("id");
        if (id == null || !id.canConvertToInt()) {
            throw new IOException("Latest schema response has no integer id for subject " + subject);
        }
        return id.intValue();
    }

    private static String requiredEnvironmentVariable(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Required environment variable is not set: " + name);
        }
        return value;
    }

    private static final class Arguments {
        private String where = DEFAULT_WHERE;
        private Integer limit;
        private String orderBy;
        private String subject = "alert-packet";
        private String schemaRegistry = DEFAULT_SCHEMA_REGISTRY;

        private static Arguments parse(String[] args) {
            Arguments options = new Arguments();
            for (int index = 0; index < args.length; index++) {
                String option = args[index];
                if (option.equals("--help")) {
                    printUsage();
                    System.exit(0);
                }
                if (index + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for " + option);
                }
                String value = args[++index];
                switch (option) {
                    case "--where" -> options.where = value;
                    case "--limit" -> options.limit = Integer.parseInt(value);
                    case "--order-by" -> options.orderBy = value;
                    case "--subject" -> options.subject = value;
                    case "--schema-registry" -> options.schemaRegistry = value;
                    default -> throw new IllegalArgumentException("Unknown option: " + option);
                }
            }
            return options;
        }

        private static void printUsage() {
            System.out.println("""
                    Usage: spark-submit --class org.lsst.alerts.AlertsToKafka alerts-to-kafka.jar [options]
                      --where <sql-predicate>
                      --limit <count>
                      --order-by <sql-expression>
                      --subject <schema-registry-subject>
                      --schema-registry <url>
                    """);
        }
    }
}