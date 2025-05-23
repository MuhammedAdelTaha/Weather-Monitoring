package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;

import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;

import org.apache.http.HttpHost;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.client.RestClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class SparkParquetStreamingToElasticsearch {
    private static final Logger logger = LoggerFactory.getLogger(SparkParquetStreamingToElasticsearch.class);
    private static final String INDEX_NAME = "weather_station_data";

    private final SparkSession spark;
    private final ElasticsearchClient esClient;
    private final int batchSize;

    public SparkParquetStreamingToElasticsearch(String elasticsearchHost, int elasticsearchPort, int batchSize) {
        this.spark = SparkSession.builder()
                .appName("SparkParquetStreamingToElasticsearch")
                .master("local[*]")  // Change for cluster mode
                .config("spark.sql.streaming.schemaInference", "false")
                .config("spark.sql.streaming.fileSource.log.cleaner.enabled", "true")
                .config("spark.sql.streaming.fileSource.log.compactInterval", "2")
                .getOrCreate();

        RestClient restClient = RestClient.builder(
                new HttpHost(elasticsearchHost, elasticsearchPort, "http")).build();

        RestClientTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        this.esClient = new ElasticsearchClient(transport);
        this.batchSize = batchSize;
    }

    private static StructType getSchema() {
        return new StructType(new StructField[]{
                new StructField("station_id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("s_no", DataTypes.LongType, false, Metadata.empty()),
                new StructField("battery_status", DataTypes.StringType, false, Metadata.empty()),
                new StructField("status_timestamp", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("weather", new StructType(new StructField[]{
                        new StructField("humidity", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("temperature", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("wind_speed", DataTypes.IntegerType, true, Metadata.empty())
                }), false, Metadata.empty())
        });
    }

    public void startStreaming(String parquetWatchDir) throws StreamingQueryException, TimeoutException {
        // Read streaming Parquet files with enhanced file detection
        Dataset<Row> streamingDF = spark.readStream()
                .schema(getSchema())
                .format("parquet")
                .option("recursiveFileLookup", "true")  // Watch subdirectories
                .option("maxFilesPerTrigger", "50")       // Process one file at a time
                .option("cleanSource", "off")            // Keep processed files
                .option("pathGlobFilter", "*.parquet")   // Only watch parquet files
                .load(parquetWatchDir);

        logger.info("Started streaming from directory: {}", parquetWatchDir);

        StreamingQuery query = streamingDF.writeStream()
                .trigger(Trigger.ProcessingTime("10 seconds"))  // Check for new files every 10s
                .foreachBatch((batchDF, batchId) -> {
                    if (batchDF.isEmpty()) {
                        logger.info("Batch {}: No new data", batchId);
                        return;
                    }

                    logger.info("Processing batch {} with {} records", batchId, batchDF.count());

                    List<Map<String, Object>> documents = batchDF.toJavaRDD().map(row -> {
                        Long stationId = row.getAs("station_id");
                        Long sNo = row.getAs("s_no");
                        String batteryStatus = row.getAs("battery_status").toString().toLowerCase();

                        Timestamp ts = row.getAs("status_timestamp");
                        Long timestampMillis = ts.getTime();  // convert Timestamp to long millis
                        Instant instant = Instant.ofEpochMilli(timestampMillis);

                        Row weather = row.getAs("weather");
                        Integer humidity = weather.getAs("humidity");
                        Integer temperature = weather.getAs("temperature");
                        Integer windSpeed = weather.getAs("wind_speed");

                        return Map.of(
                                "station_id", stationId,
                                "s_no", sNo,
                                "battery_status", batteryStatus,
                                "status_timestamp", instant.toString(),
                                "weather", Map.of(
                                        "humidity", humidity,
                                        "temperature", temperature,
                                        "wind_speed", windSpeed
                                )
                        );
                    }).collect();

                    logger.info("Batch {}: Preparing to index {} docs", batchId, documents.size());

                    List<BulkOperation> bulkOps = new ArrayList<>();
                    int count = 0;

                    for (Map<String, Object> doc : documents) {
                        String docId = doc.get("station_id").toString() + "_" + doc.get("s_no").toString();

                        BulkOperation op = BulkOperation.of(b -> b
                                .index(idx -> idx
                                        .index(INDEX_NAME)
                                        .id(docId)
                                        .document(doc)
                                ));
                        bulkOps.add(op);
                        count++;

                        if (count % batchSize == 0) {
                            sendBulkRequest(bulkOps);
                            bulkOps.clear();
                            logger.info("Batch {}: Indexed {} documents", batchId, count);
                        }
                    }

                    if (!bulkOps.isEmpty()) {
                        sendBulkRequest(bulkOps);
                        logger.info("Batch {}: Indexed remaining {} documents", batchId, bulkOps.size());
                    }
                })
                .option("checkpointLocation", "/tmp/spark/checkpoints/weather_station")
                .outputMode("append")
                .start();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                query.stop();
                close();
            } catch (IOException | TimeoutException e) {
                logger.error("Error during shutdown:", e);
            }
        }));

        query.awaitTermination();
    }

    private void sendBulkRequest(List<BulkOperation> bulkOps) throws IOException {
        BulkRequest bulkRequest = new BulkRequest.Builder()
                .operations(bulkOps)
                .build();

        BulkResponse response = esClient.bulk(bulkRequest);

        if (response.errors()) {
            logger.error("Bulk indexing errors occurred: {}", response.items());
        }
    }

    public void close() throws IOException {
        if (esClient != null) {
            esClient._transport().close();
        }
        if (spark != null) {
            spark.close();
        }
    }

    public static void main(String[] args) {
        String elasticsearchHost = "localhost";
        int elasticsearchPort = 9200;
        String parquetWatchDir = "./parquet_data";
        int batchSize = 100;

        // Validate directory exists
        if (!new java.io.File(parquetWatchDir).exists()) {
            logger.error("Directory {} does not exist", parquetWatchDir);
            System.exit(1);
        }

        SparkParquetStreamingToElasticsearch processor =
                new SparkParquetStreamingToElasticsearch(elasticsearchHost, elasticsearchPort, batchSize);

        try {
            logger.info("Starting streaming processor...");
            processor.startStreaming(parquetWatchDir);
        } catch (Exception e) {
            logger.error("Error running Spark streaming: ", e);
        } finally {
            try {
                processor.close();
            } catch (IOException e) {
                logger.error("Error closing resources: ", e);
            }
        }
    }
}