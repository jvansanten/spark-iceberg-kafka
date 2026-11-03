package org.lsst.alerts;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Standalone test for AlertsToNextcloud ParquetWriter + HTTP machinery.
 * Does NOT require Spark - tests pure Java functionality.
 * 
 * Run with: mvn test -Dtest=TestParquetHttpStreaming
 * 
 * Or with tokens:
 *   mvn exec:java -Dexec.mainClass="org.lsst.alerts.TestParquetHttpStreaming" \
 *                 -Dexec.args="--base-url https://syncandshare.desy.de --upload-token TOKEN --download-token TOKEN"
 */

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.*;

public class TestParquetHttpStreaming {

    public static void main(String[] args) throws Exception {
        // Parse arguments
        String baseUrl = "https://syncandshare.desy.de";
        String uploadToken = "test_upload_token";
        String downloadToken = "test_download_token";
        boolean testRealUpload = false;
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--base-url":
                    baseUrl = args[++i];
                    break;
                case "--upload-token":
                    uploadToken = args[++i];
                    break;
                case "--download-token":
                    downloadToken = args[++i];
                    break;
                case "--test-upload":
                    testRealUpload = true;
                    break;
                case "--help":
                    printUsage();
                    System.exit(0);
                    break;
                default:
                    System.err.println("Unknown option: " + args[i]);
                    printUsage();
                    System.exit(1);
            }
        }

        System.out.println("=== Testing Parquet Writer + HTTP Machinery ===");
        System.out.println("Base URL: " + baseUrl);
        System.out.println("Upload Token: " + (uploadToken.length() > 8 ? 
            uploadToken.substring(0, 4) + "..." + uploadToken.substring(uploadToken.length() - 4) : uploadToken));
        System.out.println("Download Token: " + (downloadToken.length() > 8 ? 
            downloadToken.substring(0, 4) + "..." + downloadToken.substring(downloadToken.length() - 4) : downloadToken));
        System.out.println();

        int passed = 0;
        int failed = 0;

        TestParquetHttpStreaming testInstance = new TestParquetHttpStreaming();

        // Test 1: ParquetBufferedWriter
        try {
            testInstance.testParquetBufferedWriter();
            System.out.println("✅ [PASS] ParquetBufferedWriter test");
            passed++;
        } catch (Exception e) {
            System.err.println("❌ [FAIL] ParquetBufferedWriter test: " + e.getMessage());
            e.printStackTrace();
            failed++;
        }

        // Test 2: URL Construction
        try {
            testInstance.testUrlConstruction();
            System.out.println("✅ [PASS] URL Construction test");
            passed++;
        } catch (Exception e) {
            System.err.println("❌ [FAIL] URL Construction test: " + e.getMessage());
            failed++;
        }

        // Test 3: HTTP Client Building
        try {
            testInstance.testHttpClient();
            System.out.println("✅ [PASS] HTTP Client test");
            passed++;
        } catch (Exception e) {
            System.err.println("❌ [FAIL] HTTP Client test: " + e.getMessage());
            failed++;
        }

        // Test 4: Schema Conversion
        try {
            testInstance.testSchemaConversion();
            System.out.println("✅ [PASS] Schema Conversion test");
            passed++;
        } catch (Exception e) {
            System.err.println("❌ [FAIL] Schema Conversion test: " + e.getMessage());
            failed++;
        }

        // Test 5: Row Conversion
        try {
            testInstance.testRowConversion();
            System.out.println("✅ [PASS] Row Conversion test");
            passed++;
        } catch (Exception e) {
            System.err.println("❌ [FAIL] Row Conversion test: " + e.getMessage());
            failed++;
        }

        // Test 6: Complex Types
        try {
            testInstance.testComplexTypes();
            System.out.println("✅ [PASS] Complex Types test");
            passed++;
        } catch (Exception e) {
            System.err.println("❌ [FAIL] Complex Types test: " + e.getMessage());
            failed++;
        }

        // Test 7: Real Upload (only if enabled and valid tokens provided)
        if (testRealUpload && !"test_upload_token".equals(uploadToken)) {
            try {
                testInstance.testRealUpload(baseUrl, uploadToken, downloadToken);
                System.out.println("✅ [PASS] Real Upload test");
                passed++;
            } catch (Exception e) {
                System.err.println("❌ [FAIL] Real Upload test: " + e.getMessage());
                e.printStackTrace();
                failed++;
            }
        } else if (testRealUpload) {
            System.out.println("⚠️  [SKIP] Real Upload test (using default test token - specify real token with --upload-token)");
        }

        System.out.println();
        System.out.println("==========================================");
        System.out.println("Results: " + passed + " passed, " + failed + " failed");
        System.out.println("==========================================");

        if (failed > 0) {
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: TestParquetHttpStreaming [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --base-url <url>           Nextcloud base URL (default: https://syncandshare.desy.de)");
        System.out.println("  --upload-token <token>    Upload/share token for WebDAV");
        System.out.println("  --download-token <token>  Read-only token for downloads");
        System.out.println("  --test-upload             Enable real upload test to Nextcloud");
        System.out.println("  --help                    Show this help");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  # Run all tests (mock only)");
        System.out.println("  mvn test -Dtest=TestParquetHttpStreaming");
        System.out.println();
        System.out.println("  # Run with real upload test");
        System.out.println("  mvn exec:java -Dexec.mainClass=\"org.lsst.alerts.TestParquetHttpStreaming\" \\");
        System.out.println("               -Dexec.args=\"--base-url https://nextcloud.your.org \\");
        System.out.println("                          --upload-token YOUR_TOKEN \\");
        System.out.println("                          --download-token READ_TOKEN \\");
        System.out.println("                          --test-upload\"");
    }

    @Test
    void testParquetBufferedWriter() throws Exception {
        System.out.println("[Test 1] ParquetBufferedWriter with ByteArrayOutputStream...");
        
        // Create Spark schema
        StructType sparkSchema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("score", DataTypes.DoubleType, false)
        });

        // Convert to Parquet schema
        MessageType schema = AlertsToNextcloud.convertToParquetSchema(sparkSchema);

        // Create test data as Rows
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, "Alice", 95.5),
                RowFactory.create(2, "Bob", 87.3),
                RowFactory.create(3, "Charlie", 92.1)
        );

        // Write to byte array via ParquetBufferedWriter
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        AlertsToNextcloud.ParquetBufferedWriter parquetWriter = new AlertsToNextcloud.ParquetBufferedWriter(baos);

        try (org.apache.parquet.hadoop.ParquetWriter<Group> pw = 
                org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(parquetWriter)
                        .withType(schema)
                        .withCompressionCodec(CompressionCodecName.ZSTD)
                        .build()) {
            
            for (Row row : rows) {
                Group group = AlertsToNextcloud.convertRowToGroup(row, schema);
                pw.write(group);
            }
        }

        byte[] parquetBytes = baos.toByteArray();
        System.out.println("  Written " + parquetBytes.length + " bytes of Parquet data");
        
        if (parquetBytes.length < 100) {
            throw new IOException("Parquet file too small: " + parquetBytes.length + " bytes");
        }
    }

    @Test
    void testUrlConstruction() throws Exception {
        System.out.println("[Test 2] URL Construction...");
        
        String baseUrl = "https://syncandshare.desy.de";
        String uploadToken = "test_upload_token";
        String downloadToken = "test_download_token";
        String filename = "test_file_" + System.currentTimeMillis() + ".parquet";
        
        String webDavUrl = AlertsToNextcloud.buildWebDavUrl(baseUrl, uploadToken, filename);
        String downloadUrl = AlertsToNextcloud.buildDownloadUrl(baseUrl, downloadToken, filename);

        System.out.println("  WebDAV URL: " + webDavUrl);
        System.out.println("  Download URL: " + downloadUrl);

        // Validate WebDAV URL
        if (!webDavUrl.startsWith(baseUrl)) {
            throw new Exception("WebDAV URL doesn't start with base URL");
        }
        if (!webDavUrl.contains("/public.php/dav/files/" + uploadToken + "/")) {
            throw new Exception("WebDAV URL has incorrect format");
        }
        if (!webDavUrl.endsWith("/" + filename)) {
            throw new Exception("WebDAV URL doesn't end with filename");
        }

        // Validate Download URL (DESY format: /download?path={encoded_path})
        if (!downloadUrl.startsWith(baseUrl)) {
            throw new Exception("Download URL doesn't start with base URL");
        }
        if (!downloadUrl.contains("/index.php/s/" + downloadToken + "/download?path=")) {
            throw new Exception("Download URL has incorrect format");
        }
        if (!downloadUrl.contains(java.net.URLEncoder.encode("/" + filename, java.nio.charset.StandardCharsets.UTF_8))) {
            throw new Exception("Download URL doesn't contain encoded filename");
        }
    }

    @Test
    void testHttpClient() throws Exception {
        System.out.println("[Test 3] HTTP Client Building...");
        
        String baseUrl = "https://syncandshare.desy.de";
        String uploadToken = "test_upload_token";
        HttpClient client = AlertsToNextcloud.createHttpClient();
        if (client == null) {
            throw new Exception("HttpClient is null");
        }

        String webDavUrl = AlertsToNextcloud.buildWebDavUrl(baseUrl, uploadToken, "test.parquet");
        HttpRequest request = AlertsToNextcloud.buildPutRequest(client, webDavUrl, uploadToken, HttpRequest.BodyPublishers.noBody());

        if (!request.method().equals("PUT")) {
            throw new Exception("Method should be PUT, got: " + request.method());
        }
        
        String xmlHttpRequestHeader = request.headers().firstValue("X-Requested-With").orElse("");
        if (!xmlHttpRequestHeader.equals("XMLHttpRequest")) {
            throw new Exception("Missing or incorrect X-Requested-With header: " + xmlHttpRequestHeader);
        }

        String authHeader = request.headers().firstValue("Authorization").orElse("");
        if (!authHeader.startsWith("Basic ")) {
            throw new Exception("Missing or incorrect Authorization header");
        }
        
        System.out.println("  HTTP Method: " + request.method());
        System.out.println("  X-Requested-With: " + xmlHttpRequestHeader);
        System.out.println("  Authorization: " + authHeader.substring(0, Math.min(10, authHeader.length())) + "...");
    }

    @Test
    void testSchemaConversion() throws Exception {
        System.out.println("[Test 4] Schema Conversion (simple types)...");
        
        // Create Spark-like schema with various primitive types
        StructType sparkSchema = new StructType(new StructField[]{
                DataTypes.createStructField("bool_col", DataTypes.BooleanType, false),
                DataTypes.createStructField("int_col", DataTypes.IntegerType, false),
                DataTypes.createStructField("long_col", DataTypes.LongType, false),
                DataTypes.createStructField("float_col", DataTypes.FloatType, false),
                DataTypes.createStructField("double_col", DataTypes.DoubleType, false),
                DataTypes.createStructField("string_col", DataTypes.StringType, true),
                DataTypes.createStructField("binary_col", DataTypes.BinaryType, true)
        });

        MessageType parquetSchema = AlertsToNextcloud.convertToParquetSchema(sparkSchema);

        if (parquetSchema.getFieldCount() != 7) {
            throw new Exception("Expected 7 fields, got: " + parquetSchema.getFieldCount());
        }

        // Verify field names
        String[] expectedNames = {"bool_col", "int_col", "long_col", "float_col", "double_col", "string_col", "binary_col"};
        for (int i = 0; i < expectedNames.length; i++) {
            if (!parquetSchema.getType(i).getName().equals(expectedNames[i])) {
                throw new Exception("Field " + i + " name mismatch: expected " + 
                        expectedNames[i] + ", got " + parquetSchema.getType(i).getName());
            }
        }

        System.out.println("  Converted " + sparkSchema.fieldNames().length + " Spark fields to Parquet");
    }

    @Test
    void testRowConversion() throws Exception {
        System.out.println("[Test 5] Row Conversion...");
        
        // Create a schema
        StructType sparkSchema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("active", DataTypes.BooleanType, false)
        });

        MessageType parquetSchema = AlertsToNextcloud.convertToParquetSchema(sparkSchema);

        // Create a row
        Row row = RowFactory.create(42, "Test Name", true);

        // Convert to Group
        Group group = AlertsToNextcloud.convertRowToGroup(row, parquetSchema);

        if (group == null) {
            throw new Exception("Group is null");
        }

        System.out.println("  Row -> Group conversion successful");
    }

    @Test
    void testComplexTypes() throws Exception {
        System.out.println("[Test 6] Complex Types (Arrays, Maps, Structs)...");
        
        // Create schema with complex types
        StructType sparkSchema = new StructType(new StructField[]{
                DataTypes.createStructField("tags", 
                        DataTypes.createArrayType(DataTypes.StringType, false), false),
                DataTypes.createStructField("metadata",
                        new MapType(DataTypes.StringType, DataTypes.IntegerType, false), false),
                DataTypes.createStructField("address",
                        new StructType(new StructField[]{
                                DataTypes.createStructField("street", DataTypes.StringType, false),
                                DataTypes.createStructField("city", DataTypes.StringType, false)
                        }), false)
        });

        MessageType parquetSchema = AlertsToNextcloud.convertToParquetSchema(sparkSchema);

        if (parquetSchema.getFieldCount() != 3) {
            throw new Exception("Expected 3 fields, got: " + parquetSchema.getFieldCount());
        }

        // Create a row with complex data
        List<String> tags = Arrays.asList("tag1", "tag2", "tag3");
        Map<String, Integer> metadata = Map.of("size", 100, "count", 50);
        Row address = RowFactory.create("Main St", "Berlin");
        Row row = RowFactory.create(tags, metadata, address);

        // Convert to Group
        Group group = AlertsToNextcloud.convertRowToGroup(row, parquetSchema);

        if (group == null) {
            throw new Exception("Group is null");
        }

        System.out.println("  Complex type conversion successful");
    }

    /**
     * Test real upload to Nextcloud via WebDAV
     * Only runs when explicitly enabled with --test-upload flag
     */
    void testRealUpload(String baseUrl, String uploadToken, String downloadToken) throws Exception {
        System.out.println("[Test 7] Real Upload to Nextcloud...");
        
        // Generate unique filename to avoid conflicts
        String filename = "test_upload_" + UUID.randomUUID().toString() + ".parquet";
        String webDavUrl = AlertsToNextcloud.buildWebDavUrl(baseUrl, uploadToken, filename);
        String downloadUrl = AlertsToNextcloud.buildDownloadUrl(baseUrl, downloadToken, filename);
        
        System.out.println("  Uploading to: " + webDavUrl);
        System.out.println("  Expected download URL: " + downloadUrl);

        // Create test schema and data
        StructType sparkSchema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, false)
        });

        MessageType parquetSchema = AlertsToNextcloud.convertToParquetSchema(sparkSchema);
        
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, "Test Row 1", 100.5),
                RowFactory.create(2, "Test Row 2", 200.3),
                RowFactory.create(3, "Test Row 3", 300.7)
        );

        // Create HTTP client and request
        HttpClient client = AlertsToNextcloud.createHttpClient();
        
        // Use PipedInputStream/PipedOutputStream to stream Parquet data
        PipedInputStream pipedIn = new PipedInputStream();
        PipedOutputStream pipedOut = new PipedOutputStream(pipedIn);
        
        AlertsToNextcloud.ParquetBufferedWriter parquetWriter = 
                new AlertsToNextcloud.ParquetBufferedWriter(pipedOut);
        
        // Start a thread to write Parquet data
        Thread writerThread = new Thread(() -> {
            try (org.apache.parquet.hadoop.ParquetWriter<Group> pw = 
                    org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(parquetWriter)
                            .withType(parquetSchema)
                            .withCompressionCodec(CompressionCodecName.ZSTD)
                            .build()) {
                for (Row row : rows) {
                    Group group = AlertsToNextcloud.convertRowToGroup(row, parquetSchema);
                    pw.write(group);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    pipedOut.close();
                } catch (IOException e) {
                    // Ignore close exception
                }
            }
        });
        
        writerThread.start();

        // Build and send HTTP PUT request with streaming body
        HttpRequest request = AlertsToNextcloud.buildPutRequest(
                client, webDavUrl, uploadToken,
                HttpRequest.BodyPublishers.ofInputStream(() -> pipedIn)
        );
        
        System.out.println("  Sending PUT request...");
        java.net.http.HttpResponse<Void> response = client.send(request, java.net.http.HttpResponse.BodyHandlers.discarding());
        
        // Wait for writer thread to finish
        writerThread.join();
        
        int statusCode = response.statusCode();
        System.out.println("  Response status: " + statusCode);
        
        if (statusCode >= 200 && statusCode < 300) {
            System.out.println("  ✓ File uploaded successfully!");
            System.out.println("  Download URL: " + downloadUrl);
            
            // Try to verify the file exists by attempting to download it
            // Need a client that follows redirects
            HttpClient redirectClient = HttpClient.newBuilder()
                    .followRedirects(java.net.http.HttpClient.Redirect.NORMAL)
                    .build();
            
            try {
                HttpRequest downloadRequest = HttpRequest.newBuilder()
                        .uri(URI.create(downloadUrl))
                        .GET()
                        .header("X-Requested-With", "XMLHttpRequest")
                        .build();
                
                java.net.http.HttpResponse<InputStream> downloadResponse = redirectClient.send(
                        downloadRequest, java.net.http.HttpResponse.BodyHandlers.ofInputStream());
                
                if (downloadResponse.statusCode() == 200) {
                    // Read the content to count bytes
                    InputStream is = downloadResponse.body();
                    long fileSize = 0;
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = is.read(buffer)) != -1) {
                        fileSize += bytesRead;
                    }
                    is.close();
                    System.out.println("  ✓ File verified via download URL (size: " + fileSize + " bytes)");
                } else {
                    System.out.println("  ⚠️  Could not verify download (final status: " + downloadResponse.statusCode() + ", uri: " + downloadResponse.uri() + ")");
                    downloadResponse.body().close();
                }
            } catch (Exception e) {
                System.out.println("  ⚠️  Could not verify download: " + e.getMessage());
            }
            
        } else {
            throw new IOException("Upload failed with status code: " + statusCode + 
                    ", response: " + response.body());
        }
    }
}
