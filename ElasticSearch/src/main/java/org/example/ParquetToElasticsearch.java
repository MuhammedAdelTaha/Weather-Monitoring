package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.avro.generic.GenericRecord;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.apache.http.HttpHost;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import java.util.ArrayList;
import java.util.List;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static org.elasticsearch.client.RestClient.builder;

public class ParquetToElasticsearch {
    private static final Logger logger = LoggerFactory.getLogger(ParquetToElasticsearch.class);
    private static final String INDEX_NAME = "weather_station_data";
    private final RestHighLevelClient esClient;
    private final String parquetBasePath;
    private final int batchSize;

    public ParquetToElasticsearch(String elasticsearchHost, int elasticsearchPort, String parquetBasePath, int batchSize) {
        this.esClient = new RestHighLevelClient(
                builder(new HttpHost(elasticsearchHost, elasticsearchPort, "http")));
        this.parquetBasePath = parquetBasePath;
        this.batchSize = batchSize;
    }


    public void setupIndex() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(INDEX_NAME);
        boolean indexExists = esClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);

        if (!indexExists) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME);

            // Define mapping for the weather station data
            String mapping = """
                    {
                      "mappings": {
                        "properties": {
                          "station_id": { "type": "keyword" },
                          "s_no": { "type": "long" },
                          "battery_status": { "type": "keyword" },
                          "status_timestamp": { "type": "date" },
                          "record_date": { "type": "date", "format": "yyyy-MM-dd" },
                          "humidity": { "type": "integer" },
                          "temperature": { "type": "integer" },
                          "wind_speed": { "type": "integer" },
                          "is_dropped": { "type": "boolean" },
                          "is_low_battery": { "type": "boolean" }
                        }
                      }
                    }""";

            createIndexRequest.source(mapping, XContentType.JSON);
            CreateIndexResponse createIndexResponse = esClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

            if (createIndexResponse.isAcknowledged()) {
                logger.info("Index {} created successfully", INDEX_NAME);
            } else {
                logger.error("Failed to create index {}", INDEX_NAME);
            }
        } else {
            logger.info("Index {} already exists", INDEX_NAME);
        }
    }

    private List<String> findParquetFiles() throws IOException {
        List<String> parquetFiles = new ArrayList<>();

        try (Stream<java.nio.file.Path> paths = Files.walk(Paths.get(parquetBasePath))) {
            paths.filter(path -> path.toString().endsWith(".parquet"))
                    .forEach(path -> parquetFiles.add(path.toString()));
        }

        return parquetFiles;
    }

    public void processAllParquetFiles() throws IOException {
        List<String> parquetFiles = findParquetFiles();
        logger.info("Found {} Parquet files to process", parquetFiles.size());

        Map<Long, Long> lastSequenceByStation = new HashMap<>();

        for (String parquetFile : parquetFiles) {
            processParquetFile(parquetFile, lastSequenceByStation);
        }
    }

    private void processParquetFile(String parquetFilePath, Map<Long, Long> lastSequenceByStation) throws IOException {
        Path path = new Path(parquetFilePath);
        BulkRequest bulkRequest = new BulkRequest();
        int recordCount = 0;

        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(path)
                .withConf(new Configuration())
                .build()) {

            GenericRecord record;
            while ((record = reader.read()) != null) {
                Long stationId = (Long) record.get("station_id");
                Long sequenceNumber = (Long) record.get("s_no");
                String batteryStatus = record.get("battery_status").toString();
                Long timestamp = (Long) record.get("status_timestamp");

                GenericRecord weatherData = (GenericRecord) record.get("weather");
                Integer humidity = (Integer) weatherData.get("humidity");
                Integer temperature = (Integer) weatherData.get("temperature");
                Integer windSpeed = (Integer) weatherData.get("wind_speed");

                boolean isDropped = false;
                if (lastSequenceByStation.containsKey(stationId)) {
                    long expectedSequence = lastSequenceByStation.get(stationId) + 1;
                    if (sequenceNumber > expectedSequence) {
                        // There's a gap in sequence numbers
                        isDropped = true;
                    }
                }

                lastSequenceByStation.put(stationId, sequenceNumber);

                LocalDateTime dateTime = Instant.ofEpochSecond(timestamp)
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
                String recordDate = dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE);

                boolean isLowBattery = "Low".equalsIgnoreCase(batteryStatus);

                Map<String, Object> document = new HashMap<>();
                document.put("station_id", stationId);
                document.put("s_no", sequenceNumber);
                document.put("battery_status", batteryStatus);
                document.put("status_timestamp", timestamp * 1000); // Convert to milliseconds for ES
                document.put("record_date", recordDate);
                document.put("humidity", humidity);
                document.put("temperature", temperature);
                document.put("wind_speed", windSpeed);
                document.put("is_dropped", isDropped);
                document.put("is_low_battery", isLowBattery);

                String docId = stationId + "_" + sequenceNumber;
                bulkRequest.add(new IndexRequest(INDEX_NAME)
                        .id(docId)
                        .source(document));

                recordCount++;

                if (recordCount % batchSize == 0) {
                    sendBulkRequest(bulkRequest);
                    bulkRequest = new BulkRequest();
                    logger.info("Processed {} records from {}", recordCount, parquetFilePath);
                }
            }

            // Send any remaining documents
            if (bulkRequest.numberOfActions() > 0) {
                sendBulkRequest(bulkRequest);
            }
            logger.info("Finished processing {} records from {}", recordCount, parquetFilePath);
        }
    }


    private void sendBulkRequest(BulkRequest bulkRequest) throws IOException {
        if (bulkRequest.numberOfActions() == 0) {
            return;
        }

        BulkResponse bulkResponse = esClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        if (bulkResponse.hasFailures()) {
            logger.error("Bulk request has failures: {}", bulkResponse.buildFailureMessage());
        }
    }

    public void close() throws IOException {
        if (esClient != null) {
            esClient.close();
        }
    }

    public static void main(String[] args) {
        String elasticsearchHost = "localhost";
        int elasticsearchPort = 9200;
        String parquetBasePath = "/var/lib/docker/volumes/parquet_archive/_data";
        int batchSize = 100;

        ParquetToElasticsearch processor = new ParquetToElasticsearch(
                elasticsearchHost, elasticsearchPort, parquetBasePath, batchSize);

        try {
            processor.setupIndex();
            processor.processAllParquetFiles();
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