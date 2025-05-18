package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class Main {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {

        String kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (kafkaBootstrapServers == null)
            kafkaBootstrapServers = "kafka:9092";

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> weatherStream = builder.stream("weather-data");

        KStream<String, String> rainingAlerts = weatherStream
                .filter((key, value) -> {
                    try {
                        JsonNode jsonNode = objectMapper.readTree(value);
                        JsonNode weatherNode = jsonNode.get("weather");
                        if (weatherNode == null) return false;
                        double humidity = weatherNode.get("humidity").asDouble();
                        return humidity > 70.0;
                    } catch (Exception e) {
                        System.err.println("Failed to parse message: " + e.getMessage());
                        return false;
                    }
                })
                .mapValues(value -> "Raining alert! High humidity detected: " + value);

        rainingAlerts.to("raining-alerts");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        // Add shutdown hook to close the stream gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
        System.out.println("Rain Detector Kafka Streams app started...");
    }
}
