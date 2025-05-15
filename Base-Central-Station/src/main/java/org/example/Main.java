package org.example;

public class Main {
    public static void main(String[] args) {
        // Dynamic configuration
        String kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (kafkaBootstrapServers == null)
            kafkaBootstrapServers = "kafka:9092";

        String applicationId = System.getenv("APP_ID");
        if (applicationId == null)
            applicationId = "central-station-app";

        String inputTopic = System.getenv("INPUT_TOPIC");
        if (inputTopic == null)
            inputTopic = "weather-data";

        String databaseDir = System.getenv("BITCASK_DB_DIR");
        if (databaseDir == null)
            databaseDir = "bitcask_db";

        String serverPortenv = System.getenv("SERVER_PORT");
        int serverPort = 8080;
        if (serverPortenv != null)
            serverPort = Integer.parseInt(serverPortenv);

        CentralStation centralStation = new CentralStation(kafkaBootstrapServers, applicationId, inputTopic, databaseDir);
        centralStation.startKafkaStreams();

        CentralStationServer server = new CentralStationServer(serverPort, centralStation.getBitCask());
        server.start();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
    }
}
