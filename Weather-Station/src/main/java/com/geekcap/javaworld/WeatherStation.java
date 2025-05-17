package com.geekcap.javaworld;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WeatherStation {
    private final long stationId;
    private long sequenceNumber;
    private final String kafkaBootstrapServers;
    private final Random random;
    private final ObjectMapper objectMapper;

    public WeatherStation(long stationId, String kafkaBootstrapServers) {
        this.stationId = stationId;
        this.sequenceNumber = 1;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.random = new Random();
        this.objectMapper = new ObjectMapper();
    }

    public void run() {
        // Configure the Kafka producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "weather-station-" + stationId);

        String topic = "weather-data";
        System.out.println("Weather Station " + stationId + " started. Sending data to Kafka topic: " + topic);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            while (true) {
                try {
                    // Randomly drop messages at 10% rate
                    if (random.nextDouble() < 0.1) {
                        System.out.println("Station " + stationId + " - Dropped message " + sequenceNumber);
                        sequenceNumber++;
                        continue;
                    }

                    // Create weather message
                    WeatherMessage message = createWeatherMessage();

                    // Convert message to JSON string
                    String messageJson = objectMapper.writeValueAsString(message);

                    // Send message to Kafka
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            topic,
                            String.valueOf(stationId),
                            messageJson
                    );

                    producer.send(record, (metadata, e) -> {
                        if (e != null)
                            System.out.println("Message delivery failed: " + e.getMessage());
                        else
                            System.out.println("Message delivered to " + metadata.topic() +
                                    " [" + metadata.partition() + "] at offset " + metadata.offset());
                    }).get();

                    System.out.println("Station " + stationId + " - Sent message " + sequenceNumber + ": " + messageJson);

                    // Increment sequence number
                    sequenceNumber++;

                    // Wait for 1 second before next reading
                    Thread.sleep(1000);

                } catch (InterruptedException e) {
                    System.out.println("Weather Station " + stationId + " interrupted");
                    break;
                } catch (ExecutionException e) {
                    System.out.println("Error sending message: " + e.getMessage());
                } catch (Exception e) {
                    System.out.println("Unexpected error: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.out.println("Error initializing producer: " + e.getMessage());
        } finally {
            System.out.println("Weather Station " + stationId + " stopped");
        }
    }

    private WeatherMessage createWeatherMessage() {
        // Battery status with probabilities: low=30%, medium=40%, high=30%
        String batteryStatus;
        double batteryRandom = random.nextDouble();
        if (batteryRandom < 0.3)
            batteryStatus = "low";
        else if (batteryRandom < 0.7)
            batteryStatus = "medium";
        else
            batteryStatus = "high";

        // Weather data with random values
        int humidity = random.nextInt(91) + 10;     // 10-100 percentage
        int temperature = random.nextInt(79) + 32;  // 32-110 Fahrenheit
        int windSpeed = random.nextInt(61);         // 0-60 km/h

        // Create message
        WeatherMessage message = new WeatherMessage();
        Map<String, Integer> weather = new HashMap<>();
        weather.put("humidity", humidity);
        weather.put("temperature", temperature);
        weather.put("wind_speed", windSpeed);

        message.stationId = this.stationId;
        message.sequenceNumber = this.sequenceNumber;
        message.batteryStatus = batteryStatus;
        message.statusTimestamp = ZonedDateTime.now(ZoneId.systemDefault()).toEpochSecond() * 1000L;
        message.weather = weather;

        return message;
    }

    static class WeatherMessage {
        public long stationId;
        public long sequenceNumber;
        public String batteryStatus;
        public long statusTimestamp;
        public Map<String, Integer> weather;

        // Default constructor required for Jackson
        public WeatherMessage() {}
    }
}
