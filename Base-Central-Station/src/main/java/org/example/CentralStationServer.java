package org.example;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.plugin.bundled.CorsPluginConfig;

import java.util.Map;

public class CentralStationServer {

    private final int port;
    private final BitCask bitCask;
    private Javalin server;

    public CentralStationServer(int port, BitCask bitCask) {
        this.port = port;
        this.bitCask = bitCask;
    }

    public void start() {
        server = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
            config.plugins.enableCors(cors -> cors.add(CorsPluginConfig::anyHost));
        });

        server.get("/stations", this::handleGetAllStations);
        server.get("/station", this::handleGetStation);

        server.start(port);
        System.out.println("Central Station HTTP Server started on port " + port);
    }

    private void handleGetAllStations(Context ctx) {
        Map<String, String> allData = bitCask.getAll();
        ctx.json(allData);  // Javalin handles JSON serialization automatically
    }

    private void handleGetStation(Context ctx) {
        String stationId = ctx.queryParam("id");
        if (stationId == null || stationId.isEmpty()) {
            ctx.status(400).json(Map.of("error", "Missing 'id' parameter"));
            return;
        }

        String data = bitCask.get(stationId);
        if (data == null) {
            ctx.status(404).json(Map.of("error", "Station not found"));
        } else {
            ctx.json(data);
        }
    }

    public void stop() {
        if (server != null) {
            server.stop();
            bitCask.shutdown();
        }
    }
}