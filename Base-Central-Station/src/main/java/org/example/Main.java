package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        // Dynamic configuration with defaults
        String kafkaBootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String applicationId = getEnv("APP_ID", "central-station-app");
        String inputTopic = getEnv("INPUT_TOPIC", "weather-data");
        String databaseDir = getEnv("BITCASK_DB_DIR", "/data/bitcask_db");
        String archiveDir = getEnv("ARCHIVE_DIR", "/data/parquet_archive");
        int serverPort = getEnvAsInt("SERVER_PORT", 8080);

        logger.info("Starting Central Station with configuration:");
        logger.info("Kafka: {}", kafkaBootstrapServers);
        logger.info("App ID: {}", applicationId);
        logger.info("Input Topic: {}", inputTopic);
        logger.info("Bitcask DB Dir: {}", databaseDir);
        logger.info("Archive Dir: {}", archiveDir);
        logger.info("Server Port: {}", serverPort);

        // Initialize components
        CentralStation centralStation = new CentralStation(
                kafkaBootstrapServers,
                applicationId,
                inputTopic,
                databaseDir,
                archiveDir
        );

        CentralStationServer server = new CentralStationServer(serverPort, centralStation.getBitCask());

        // Start services
        centralStation.startKafkaStreams();
        server.start();

        // Graceful shutdown handling
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down gracefully...");
            try {
                centralStation.shutdown();
                server.stop();
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));

        logger.info("Central Station is now running");
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    private static int getEnvAsInt(String key, int defaultValue) {
        String value = System.getenv(key);
        try {
            return value != null ? Integer.parseInt(value) : defaultValue;
        } catch (NumberFormatException e) {
            logger.warn("Invalid value for {}: {}, using default {}", key, value, defaultValue);
            return defaultValue;
        }
    }
}