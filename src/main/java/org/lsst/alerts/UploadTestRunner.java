/**
 * Simple runner for upload tests - copies logic from TestParquetHttpStreaming
 * to allow execution via exec:java
 */
package org.lsst.alerts;

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
import java.net.http.HttpResponse;
import java.util.*;

public class UploadTestRunner {

    public static void main(String[] args) throws Exception {
        // Parse arguments
        String baseUrl = "https://syncandshare.desy.de";
        String uploadToken = null;
        String downloadToken = null;
        
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

        if (uploadToken == null || uploadToken.isEmpty()) {
            System.err.println("Error: --upload-token is required");
            printUsage();
            System.exit(1);
        }

        System.out.println("=== Real Upload Test to DESY Nextcloud ===");
        System.out.println("Base URL: " + baseUrl);
        System.out.println("Upload Token: " + maskToken(uploadToken));
        System.out.println("Download Token: " + (downloadToken != null ? maskToken(downloadToken) : "not provided"));
        System.out.println();

        try {
            testRealUpload(baseUrl, uploadToken, downloadToken);
            System.out.println("\n✅ UPLOAD TEST PASSED!");
        } catch (Exception e) {
            System.err.println("\n❌ UPLOAD TEST FAILED!");
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static String maskToken(String token) {
        if (token == null || token.length() <= 8) return token;
        return token.substring(0, 4) + "..." + token.substring(token.length() - 4);
    }

    private static void printUsage() {
        System.out.println("Usage: UploadTestRunner [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --base-url <url>        Nextcloud base URL");
        System.out.println("  --upload-token <token>  Upload/share token for WebDAV (REQUIRED)");
        System.out.println("  --download-token <token> Read-only token for downloads");
        System.out.println("  --help                  Show this help");
    }

    static void testRealUpload(String baseUrl, String uploadToken, String downloadToken) throws Exception {
        System.out.println("[Test] Real Upload to Nextcloud...");
        
        // Generate unique filename to avoid conflicts
        String filename = "test_upload_" + UUID.randomUUID().toString() + ".parquet";
        String webDavUrl = buildWebDavUrl(baseUrl, uploadToken, filename);
        String downloadUrl = buildDownloadUrl(baseUrl, downloadToken != null ? downloadToken : uploadToken, filename);
        
        System.out.println("  Filename: " + filename);
        System.out.println("  WebDAV URL: " + webDavUrl);
        System.out.println("  Expected download URL: " + downloadUrl);

        // Create test schema and data
        StructType sparkSchema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, false)
        });

        MessageType parquetSchema = convertToParquetSchema(sparkSchema);
        
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, "Test Row 1", 100.5),
                RowFactory.create(2, "Test Row 2", 200.3),
                RowFactory.create(3, "Test Row 3", 300.7)
        );

        // Create HTTP client
        HttpClient client = createHttpClient();
        
        // Use PipedInputStream/PipedOutputStream to stream Parquet data
        PipedInputStream pipedIn = new PipedInputStream();
        PipedOutputStream pipedOut = new PipedOutputStream(pipedIn);
        
        AlertsToNextcloud.ParquetBufferedWriter parquetWriter = new AlertsToNextcloud.ParquetBufferedWriter(pipedOut);
        
        // Start a thread to write Parquet data
        Thread writerThread = new Thread(() -> {
            try (org.apache.parquet.hadoop.ParquetWriter<Group> pw = 
                    org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(parquetWriter)
                            .withType(parquetSchema)
                            .withCompressionCodec(CompressionCodecName.ZSTD)
                            .build()) {
                for (Row row : rows) {
                    Group group = convertRowToGroup(row, parquetSchema);
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
        HttpRequest request = buildPutRequest(client, webDavUrl, uploadToken,
                HttpRequest.BodyPublishers.ofInputStream(() -> pipedIn));
        
        System.out.println("  Sending PUT request...");
        HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
        
        // Wait for writer thread to finish
        writerThread.join();
        
        int statusCode = response.statusCode();
        System.out.println("  Response status: " + statusCode);
        
        if (statusCode >= 200 && statusCode < 300) {
            System.out.println("  ✓ File uploaded successfully!");
            System.out.println("  Download URL: " + downloadUrl);
            
            // Try to verify the file exists by attempting to download it
            if (downloadToken != null) {
                try {
                    HttpRequest downloadRequest = HttpRequest.newBuilder()
                            .uri(URI.create(downloadUrl))
                            .GET()
                            .header("X-Requested-With", "XMLHttpRequest")
                            .build();
                    
                    HttpResponse<InputStream> downloadResponse = client.send(
                            downloadRequest, HttpResponse.BodyHandlers.ofInputStream());
                    
                    if (downloadResponse.statusCode() == 200) {
                        long fileSize = downloadResponse.body().available();
                        System.out.println("  ✓ File verified via download URL (size: " + fileSize + " bytes)");
                        downloadResponse.body().close();
                    } else {
                        System.out.println("  ⚠️  Could not verify download (status: " + downloadResponse.statusCode() + ")");
                    }
                } catch (Exception e) {
                    System.out.println("  ⚠️  Could not verify download: " + e.getMessage());
                }
            } else {
                System.out.println("  ℹ️  Skipping download verification (no download token provided)");
            }
            
        } else {
            throw new IOException("Upload failed with status code: " + statusCode);
        }
    }

    // Delegate to AlertsToNextcloud methods
    private static MessageType convertToParquetSchema(StructType sparkSchema) {
        return AlertsToNextcloud.convertToParquetSchema(sparkSchema);
    }

    private static Group convertRowToGroup(Row row, MessageType schema) {
        return AlertsToNextcloud.convertRowToGroup(row, schema);
    }

    private static String buildWebDavUrl(String baseUrl, String token, String filename) {
        return AlertsToNextcloud.buildWebDavUrl(baseUrl, token, filename);
    }

    private static String buildDownloadUrl(String baseUrl, String token, String filename) {
        // Use the /download?path= format for DESY Nextcloud
        String encodedPath = java.net.URLEncoder.encode("/" + filename, java.nio.charset.StandardCharsets.UTF_8);
        return baseUrl + "/index.php/s/" + token + "/download?path=" + encodedPath;
    }

    private static HttpClient createHttpClient() {
        return AlertsToNextcloud.createHttpClient();
    }

    private static HttpRequest buildPutRequest(HttpClient client, String url, String token, HttpRequest.BodyPublisher body) {
        return AlertsToNextcloud.buildPutRequest(client, url, token, body);
    }
}
