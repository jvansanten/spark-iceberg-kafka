package org.lsst.alerts;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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

    private AlertsToNextcloud() {}

    public static void main(String[] args) throws Exception {
        Arguments options = Arguments.parse(args);

        SparkSession spark = SparkSession.builder()
                .appName("AlertsToNextcloud")
                .getOrCreate();
        try {
            Dataset<Row> alerts = spark.sql(buildQuery(options));
            Dataset<Row> projected = applySchemaExclusions(alerts, options.schemaExclude);

            // TODO: stream each partition of `projected` to Nextcloud as Parquet
            // and collect the read-only download URLs produced by the executors.
            List<String> downloadUrls = List.of();

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
                + "/"
                + encodePathSegment(filename)
                + "?download";
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

    private static final class Arguments {
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
