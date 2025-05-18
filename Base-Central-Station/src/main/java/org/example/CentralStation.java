package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CentralStation {
    private final String kafkaBootstrapServers;
    private final String applicationId;
    private final String inputTopic;
    private final BitCask bitCask;
    private final ParquetArchiver parquetArchiver;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(CentralStation.class);

    public CentralStation(String kafkaBootstrapServers, String applicationId,
                          String inputTopic, String databaseDir, String archiveDir) throws IOException {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.applicationId = applicationId;
        this.inputTopic = inputTopic;
        this.bitCask = new BitCask(databaseDir);

        // Load Avro schema and initialize ParquetArchiver
        Schema schema = loadSchema();
        this.parquetArchiver = new ParquetArchiver(archiveDir, schema);
    }

    private Schema loadSchema() throws IOException {
        try (InputStream is = getClass().getResourceAsStream("/avro/WeatherStatus.avsc")) {
            if (is == null) {
                throw new IOException("Schema file not found at /avro/WeatherStatus.avsc");
            }
            logger.info("Schema file found at /avro/WeatherStatus.avsc");
            return new Schema.Parser().parse(is);
        }
    }

    public void startKafkaStreams() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> weatherStream = builder.stream(inputTopic);

        weatherStream.foreach((key, value) -> {
            try {
                var jsonNode = objectMapper.readTree(value);
                // 1. Store in BitCask
                int stationId = jsonNode.get("stationId").asInt();
                String weatherStatus = jsonNode.toString();
                bitCask.put(String.valueOf(stationId), weatherStatus);

                // 2. Archive to Parquet
                GenericRecord record = convertToAvro((ObjectNode) jsonNode);
                parquetArchiver.archive(record);

                logger.info("Processed data for station {}", stationId);
            } catch (Exception e) {
                logger.error("Error processing message: {}", e.getMessage());
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        // Add shutdown hook to clean up resources
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            parquetArchiver.shutdown();
            streams.close();
        }));

        streams.start();
        logger.info("Kafka Streams started for application {}", applicationId);
    }

    private GenericRecord convertToAvro(ObjectNode jsonNode) {
        Schema schema = parquetArchiver.getSchema();
        GenericRecord record = new GenericData.Record(schema);

        // Top-level fields
        record.put("station_id", jsonNode.get("stationId").asLong());
        record.put("s_no", jsonNode.get("sequenceNumber").asLong());
        record.put("status_timestamp", jsonNode.get("statusTimestamp").asLong());

        // Battery status enum
        record.put("battery_status",
                new GenericData.EnumSymbol(
                        schema.getField("battery_status").schema(),
                        jsonNode.get("batteryStatus").asText()
                )
        );

        // Nested weather record
        Schema weatherSchema = schema.getField("weather").schema();
        ObjectNode weatherNode = (ObjectNode) jsonNode.get("weather");
        GenericRecord weatherRecord = new GenericData.Record(weatherSchema);

        weatherRecord.put("humidity", weatherNode.get("humidity").asInt());
        weatherRecord.put("temperature", weatherNode.get("temperature").asInt());
        weatherRecord.put("wind_speed", weatherNode.get("wind_speed").asInt());

        record.put("weather", weatherRecord);

        return record;
    }

    public BitCask getBitCask() {
        return bitCask;
    }

    public void shutdown() {
        parquetArchiver.shutdown();
    }
}