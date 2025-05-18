package org.example;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

class OpenMeteoChannelAdapter {
    private final Random random = new Random();

    private String getBatteryStatus() {
        double rand = random.nextDouble();
        if (rand < 0.3) return "low";
        if (rand < 0.7) return "medium";
        return "high";
    }

    public WeatherMessage adaptData(long stationId, long sequenceNumber, Map<String, Integer> weatherData) {
        long timestamp = ZonedDateTime.now(ZoneId.systemDefault()).toEpochSecond() * 1000L;

        // Create and populate the WeatherMessage object
        WeatherMessage message = new WeatherMessage();
        message.stationId = stationId;
        message.sequenceNumber = sequenceNumber;
        message.batteryStatus = getBatteryStatus();
        message.statusTimestamp = timestamp;
        message.weather = weatherData;

        return message;
    }
}