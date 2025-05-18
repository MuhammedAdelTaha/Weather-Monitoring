package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class WeatherDataProducer {
    private final long stationId;
    private long sequenceNumber;
    private final String kafkaBootstrapServers;
    private final WeatherDataFetcher fetcher;
    private final OpenMeteoChannelAdapter adapter;
    private final Random random;

    public WeatherDataProducer(long stationId, String kafkaBootstrapServers, double latitude, double longitude) {
        this.stationId = stationId;
        this.sequenceNumber = 1;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.fetcher = new WeatherDataFetcher(latitude, longitude);
        this.adapter = new OpenMeteoChannelAdapter();
        this.random = new Random();
    }

    public void run() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "weather-station-" + stationId);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            while (true) {
                try {
                    if (random.nextDouble() < 0.1) {
                        System.out.println("Station " + stationId + " - Dropped message " + sequenceNumber);
                        sequenceNumber++;
                        continue;
                    }
                    Map<String, Integer> weatherData = fetcher.fetchWeatherData();
                    if (weatherData != null) {
                        WeatherMessage adaptedMessage = adapter.adaptData(stationId, sequenceNumber++, weatherData);
                        String messageJson = new ObjectMapper().writeValueAsString(adaptedMessage);

                        String topic = "weather-data";
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(stationId), messageJson);

                        // Async send with callback
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                System.out.println("Error sending message: " + exception.getMessage());
                            } else {
                                System.out.println("Sent message to topic " + metadata.topic() +
                                        " partition " + metadata.partition() +
                                        " offset " + metadata.offset());
                            }
                        });
                    }
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    System.out.println("Interrupted: " + e.getMessage());
                    Thread.currentThread().interrupt(); // restore interrupted status
                    break; // optional: break loop if interrupted
                } catch (Exception e) {
                    System.out.println("Unexpected error: " + e.getMessage());
                }
            }
        }
    }
}
