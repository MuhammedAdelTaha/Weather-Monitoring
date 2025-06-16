package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.avro.generic.GenericRecord;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.transport.endpoints.BooleanResponse;

import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.time.Instant;
import java.nio.file.*;
import static java.nio.file.StandardWatchEventKinds.*;

public class ParquetToElasticsearch {
    private static final Logger logger = LoggerFactory.getLogger(ParquetToElasticsearch.class);
    private static final String INDEX_NAME = "weather_station_data";
    private final ElasticsearchClient esClient;
    private final String parquetBasePath;
    private final int batchSize;

    public ParquetToElasticsearch(String elasticsearchHost, int elasticsearchPort, String parquetBasePath, int batchSize) {
        RestClient restClient = RestClient.builder(
                new HttpHost(elasticsearchHost, elasticsearchPort, "http")).build();

        RestClientTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        this.esClient = new ElasticsearchClient(transport);
        this.parquetBasePath = parquetBasePath;
        this.batchSize = batchSize;
    }

    public void setupIndex() throws IOException {
        ExistsRequest existsRequest = new ExistsRequest.Builder()
                .index(INDEX_NAME)
                .build();
        BooleanResponse existsResponse = esClient.indices().exists(existsRequest);

        if (!existsResponse.value()) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
                    .index(INDEX_NAME)
                    .mappings(m -> m
                            .properties("station_id", p -> p.long_(l -> l))  // Changed from keyword to long to match Avro
                            .properties("s_no", p -> p.long_(l -> l))
                            .properties("battery_status", p -> p.keyword(k -> k))  // enum maps well to keyword
                            .properties("status_timestamp", p -> p.date(d -> d
                                    .format("strict_date_optional_time||epoch_millis")))
                            .properties("weather", p -> p.object(o -> o  // Nested weather object
                                    .properties("humidity", p2 -> p2.integer(i -> i))
                                    .properties("temperature", p2 -> p2.integer(i -> i))
                                    .properties("wind_speed", p2 -> p2.integer(i -> i))
                            ))
                    )
                    .build();
            CreateIndexResponse createIndexResponse = esClient.indices().create(createIndexRequest);
            if (createIndexResponse.acknowledged()) {
                logger.info("Index {} created successfully", INDEX_NAME);
            } else {
                logger.error("Failed to create index {}", INDEX_NAME);
            }
        } else {
            logger.info("Index {} already exists", INDEX_NAME);
        }
    }

    public void watchParquetDirectory() throws IOException, InterruptedException {
        WatchService watchService = FileSystems.getDefault().newWatchService();
        Map<WatchKey, Path> keyPathMap = new HashMap<>();

        // Recursively register all directories on startup
        Files.walk(Paths.get(parquetBasePath))
                .filter(Files::isDirectory)
                .forEach(path -> {
                    try {
                        WatchKey key = path.register(watchService, ENTRY_CREATE);
                        keyPathMap.put(key, path);
                        logger.info("Registered directory for watching: {}", path);
                    } catch (IOException e) {
                        logger.error("Error registering path for watch: {}", path, e);
                    }
                });

        processExistingParquetFiles();

        logger.info("Watching for new Parquet files in: {}", parquetBasePath);
        while (true) {
            WatchKey key = watchService.take(); // blocks until events are present
            Path dir = keyPathMap.get(key);
            if (dir == null) {
                logger.warn("WatchKey not recognized!");
                if (!key.reset()) {
                    break;  // Exit if key no longer valid
                }
                continue;
            }
            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                if (kind == OVERFLOW) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path name = ev.context();
                Path child = dir.resolve(name);
                if (kind == ENTRY_CREATE) {
                    if (Files.isDirectory(child)) {
                        // Recursively register new directory and all its subdirectories
                        try {
                            Files.walk(child)
                                    .filter(Files::isDirectory)
                                    .forEach(subdir -> {
                                        try {
                                            WatchKey newKey = subdir.register(watchService, ENTRY_CREATE);
                                            keyPathMap.put(newKey, subdir);
                                            logger.info("Registered new subdirectory for watching: {}", subdir);
                                        } catch (IOException e) {
                                            logger.error("Failed to register new subdirectory: {}", subdir, e);
                                        }
                                    });
                        } catch (IOException e) {
                            logger.error("Failed to walk new directory for registration: {}", child, e);
                        }
                    } else if (child.toString().endsWith(".parquet")) {
                        logger.info("New Parquet file detected: {}", child);
                        try {
                            processParquetFile(child.toString());
                        } catch (IOException e) {
                            logger.error("Failed to process new file: {}", child, e);
                        }
                    }
                }
            }
            boolean valid = key.reset();
            if (!valid) {
                keyPathMap.remove(key);
                if (keyPathMap.isEmpty()) break;
            }
        }
    }

    private void processExistingParquetFiles() throws IOException {
        Files.walk(Paths.get(parquetBasePath))
                .filter(path -> !Files.isDirectory(path))
                .filter(path -> path.toString().endsWith(".parquet"))
                .forEach(path -> {
                    logger.info("Processing existing Parquet file: {}", path);
                    try {
                        processParquetFile(path.toString());
                    } catch (IOException e) {
                        logger.error("Failed to process existing file: {}", path, e);
                    }
                });
    }

    private void processParquetFile(String parquetFilePath) throws IOException {
        // Define the path for the Parquet file
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(parquetFilePath);
        // Initialize a BulkRequest builder for batching Elasticsearch operations
        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();
        int recordCount = 0; // Counter to keep track of processed records
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(path)
                .withConf(new Configuration())
                .build()) {

            GenericRecord record;
            // Loop through all records in the Parquet file
            while ((record = reader.read()) != null) {
                // Extract document fields from the GenericRecord
                Map<String, Object> document = extractDocument(record);
                // Construct a unique document ID using station_id and s_no
                String docId = document.get("station_id").toString() + "_" + document.get("s_no").toString();
                // Add an index operation to the bulk request
                bulkBuilder.operations(op -> op
                        .index(idx -> idx
                                .index(INDEX_NAME)
                                .id(docId)
                                .document(document)
                        ));
                recordCount++;
                // Send bulk request if batch size is reached
                if (recordCount % batchSize == 0) {
                    sendBulkRequest(bulkBuilder.build());
                    bulkBuilder = new BulkRequest.Builder();
                    logger.info("Processed {} records from {}", recordCount, parquetFilePath);
                }
            }
            // Send any remaining records in the bulk request
            if (recordCount % batchSize != 0) {
                sendBulkRequest(bulkBuilder.build());
            }
            logger.info("Finished processing {} records from {}", recordCount, parquetFilePath);
        }
    }

    private Map<String, Object> extractDocument(GenericRecord record) {
        // Extract fields from the main record
        Long stationId = (Long) record.get("station_id");
        Long sequenceNumber = (Long) record.get("s_no");
        String batteryStatus = record.get("battery_status").toString(); // enum will be string
        Long timestampMillis = (Long) record.get("status_timestamp"); // already in millis
        // Extract nested weather data
        GenericRecord weatherData = (GenericRecord) record.get("weather");
        Integer humidity = (Integer) weatherData.get("humidity");
        Integer temperature = (Integer) weatherData.get("temperature");
        Integer windSpeed = (Integer) weatherData.get("wind_speed");
        // Convert timestamp to date formats
        Instant instant = Instant.ofEpochMilli(timestampMillis);
        // Build the document
        Map<String, Object> document = new HashMap<>();
        document.put("station_id", stationId);
        document.put("s_no", sequenceNumber);
        document.put("battery_status", batteryStatus.toLowerCase()); // ensure lowercase to match enum
        document.put("status_timestamp", instant.toString()); // ISO-8601 format
        // Nested weather object
        Map<String, Object> weather = new HashMap<>();
        weather.put("humidity", humidity);
        weather.put("temperature", temperature);
        weather.put("wind_speed", windSpeed);
        document.put("weather", weather);
        return document;
    }

    private void sendBulkRequest(BulkRequest bulkRequest) throws IOException {
        if (bulkRequest.operations().isEmpty()) return;

        try {
            BulkResponse bulkResponse = esClient.bulk(bulkRequest);
            if (bulkResponse.errors()) {
                logger.error("Bulk request has failures: {}", bulkResponse.items());
            }
        } catch (ElasticsearchException e) {
            logger.error("Elasticsearch exception during bulk request", e);
            throw e;
        }
    }

    public void close() throws IOException {
        if (esClient != null) {
            esClient._transport().close();
        }
    }

    public static void main(String[] args) {
        String elasticsearchHost = "localhost";
        int elasticsearchPort = 9200;
        String parquetBasePath = "/var/lib/docker/volumes/parquet_archive/_data";

        //use if running without docker
        //String parquetBasePath = "./parquet_data";

        int batchSize = 100;

        ParquetToElasticsearch processor = new ParquetToElasticsearch(
                elasticsearchHost, elasticsearchPort, parquetBasePath, batchSize);

        try {
            processor.setupIndex();
            processor.watchParquetDirectory();
            logger.info("Successfully indexed all weather station data to Elasticsearch");
        } catch (Exception e) {
            logger.error("Error processing Parquet files: ", e);
        } finally {
            try {
                processor.close();
            } catch (IOException e) {
                logger.error("Error closing Elasticsearch client: ", e);
            }
        }
    }
}
