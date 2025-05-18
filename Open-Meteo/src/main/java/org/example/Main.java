package org.example;

public class Main {
    public static void main(String[] args) {
        // Read environment variables, provide default values if not set
        long stationId = Long.parseLong(System.getenv().getOrDefault("STATION_ID", "11"));
        String kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        double latitude = Double.parseDouble(System.getenv().getOrDefault("LATITUDE", "52.52"));
        double longitude = Double.parseDouble(System.getenv().getOrDefault("LONGITUDE", "13.41"));

        WeatherDataProducer producer = new WeatherDataProducer(stationId, kafkaBootstrapServers, latitude, longitude);
        producer.run();
    }
}
