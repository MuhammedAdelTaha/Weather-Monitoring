package org.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class WeatherDataFetcher {
    private final ObjectMapper objectMapper;
    private final String apiUrl;

    public WeatherDataFetcher(double latitude, double longitude) {
        this.objectMapper = new ObjectMapper();
        this.apiUrl = String.format(
                "https://api.open-meteo.com/v1/forecast?" +
                        "latitude=%.2f&" +
                        "longitude=%.2f&" +
                        "current=temperature_2m,relative_humidity_2m,wind_speed_10m&" +
                        "temperature_unit=fahrenheit",
                latitude, longitude
        );
    }

    public Map<String, Integer> fetchWeatherData() {
        try {
            URL url = new URL(apiUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();

            JsonNode root = objectMapper.readTree(content.toString());
            JsonNode current = root.path("current");

            int humidity = current.path("relative_humidity_2m").asInt();
            int temperature = (int) Math.round(current.path("temperature_2m").asDouble());
            int windSpeed = (int) Math.round(current.path("wind_speed_10m").asDouble());

            Map<String, Integer> weather = new HashMap<>();
            weather.put("humidity", humidity);
            weather.put("temperature", temperature);
            weather.put("wind_speed", windSpeed);

            return weather;
        } catch (Exception e) {
            System.out.println("Error fetching weather data: " + e.getMessage());
            return null;
        }
    }
}