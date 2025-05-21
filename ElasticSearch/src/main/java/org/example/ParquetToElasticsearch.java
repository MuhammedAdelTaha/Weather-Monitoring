package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.stream.Stream;
import java.util.ArrayList;
import java.util.List;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

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
                    .mappings(m -> m.properties("station_id", p -> p.keyword(k -> k))
                            .properties("s_no", p -> p.long_(l -> l))
                            .properties("battery_status", p -> p.keyword(k -> k))
                            .properties("status_timestamp", p -> p.date(d -> d))
                            .properties("record_date", p -> p.date(d -> d.format("yyyy-MM-dd")))
                            .properties("humidity", p -> p.integer(i -> i))
                            .properties("temperature", p -> p.integer(i -> i))
                            .properties("wind_speed", p -> p.integer(i -> i))
                            .properties("is_dropped", p -> p.boolean_(b -> b))
                            .properties("is_low_battery", p -> p.boolean_(b -> b)))
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
        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();
        int recordCount = 0;

        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(path)
                .withConf(new Configuration())
                .build()) {

            GenericRecord record;
            while ((record = reader.read()) != null) {
                Long stationId = getLongFromRecord(record, "station_id");
                Long sequenceNumber = getLongFromRecord(record, "s_no");
                String batteryStatus = safeToString(record.get("battery_status"));
                Long timestamp = getLongFromRecord(record, "status_timestamp");

                GenericRecord weatherData = (GenericRecord) record.get("weather");
                Integer humidity = getIntFromRecord(weatherData, "humidity");
                Integer temperature = getIntFromRecord(weatherData, "temperature");
                Integer windSpeed = getIntFromRecord(weatherData, "wind_speed");

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
                document.put("status_timestamp", timestamp * 1000); // Convert to ms for ES
                document.put("record_date", recordDate);
                document.put("humidity", humidity);
                document.put("temperature", temperature);
                document.put("wind_speed", windSpeed);
                document.put("is_dropped", isDropped);
                document.put("is_low_battery", isLowBattery);

                String docId = stationId + "_" + sequenceNumber;
                bulkBuilder.operations(op -> op
                        .index(idx -> idx
                                .index(INDEX_NAME)
                                .id(docId)
                                .document(document)
                        ));

                recordCount++;

                if (recordCount % batchSize == 0) {
                    sendBulkRequest(bulkBuilder.build());
                    bulkBuilder = new BulkRequest.Builder();
                    logger.info("Processed {} records from {}", recordCount, parquetFilePath);
                }
            }

            if (!bulkBuilder.build().operations().isEmpty()) {
                sendBulkRequest(bulkBuilder.build());
            }

            logger.info("Finished processing {} records from {}", recordCount, parquetFilePath);
        }
    }

    private void sendBulkRequest(BulkRequest bulkRequest) throws IOException {
        if (bulkRequest.operations().isEmpty()) {
            return;
        }

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

    private Long getLongFromRecord(GenericRecord record, String fieldName) {
        Object val = record.get(fieldName);
        if (val instanceof Integer) {
            return ((Integer) val).longValue();
        } else if (val instanceof Long) {
            return (Long) val;
        }
        return null;
    }

    private Integer getIntFromRecord(GenericRecord record, String fieldName) {
        Object val = record.get(fieldName);
        if (val instanceof Integer) {
            return (Integer) val;
        } else if (val instanceof Long) {
            return ((Long) val).intValue();
        }
        return null;
    }

    private String safeToString(Object obj) {
        return obj == null ? "" : obj.toString();
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
