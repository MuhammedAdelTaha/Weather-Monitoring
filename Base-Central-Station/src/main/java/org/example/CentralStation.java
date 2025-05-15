package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class CentralStation {

    private final String kafkaBootstrapServers;
    private final String applicationId;
    private final String inputTopic;
    private final String databaseDir;
    private final BitCask bitCask;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public CentralStation(String kafkaBootstrapServers, String applicationId, String inputTopic, String databaseDir) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.applicationId = applicationId;
        this.inputTopic = inputTopic;
        this.databaseDir = databaseDir;
        this.bitCask = new BitCask(databaseDir);
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
                int stationId = jsonNode.get("stationId").asInt();
                String weatherStatus = jsonNode.toString();
                bitCask.put(String.valueOf(stationId), weatherStatus);
                System.out.println("Stored data for station " + stationId);
            } catch (Exception e) {
                System.err.println("Error processing message: " + e.getMessage());
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
        System.out.println("Kafka Streams started...");
    }

    public BitCask getBitCask() {
        return bitCask;
    }
}
