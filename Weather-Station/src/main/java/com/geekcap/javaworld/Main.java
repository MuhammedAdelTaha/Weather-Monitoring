package com.geekcap.javaworld;

public class Main {
    public static void main(String[] args) {
        // Get station ID from command line or environment variable
        long stationId = 1;
        if (args.length > 0)
            stationId = Long.parseLong(args[0]);
        else if (System.getenv("STATION_ID") != null)
            stationId = Long.parseLong(System.getenv("STATION_ID"));

        // Get Kafka bootstrap servers from environment variable or use default
        String kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (kafkaBootstrapServers == null)
            kafkaBootstrapServers = "kafka:9092";

        WeatherStation weatherStation = new WeatherStation(stationId, kafkaBootstrapServers);
        weatherStation.run();
    }
}
