package org.example;

import java.util.Map;

class WeatherMessage {
    public long stationId;
    public long sequenceNumber;
    public String batteryStatus;
    public long statusTimestamp;
    public Map<String, Integer> weather;

    // Default constructor required for Jackson
    public WeatherMessage() {}
}
