package com.distributedsystems.getclient;

import com.distributedsystems.shared.AggregationServerClient;
import com.distributedsystems.shared.HttpHelper;
import com.distributedsystems.shared.SimpleJsonUtil;

import java.net.*;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class GetClient extends AggregationServerClient {

    public HttpHelper.Response requestStationData(String stationId) throws IOException, InterruptedException {
        clock.tick();
        HttpHelper.Response response = null;
        for (int i = 0; i < MAX_REQUEST_ATTEMPTS; i++) {
            Map<String, String> headers = new HashMap<>();
            headers.put("X-Lamport-Clock", String.valueOf(clock.get()));
            if (stationId != null) headers.put("stationId", stationId);

            // initially 0 seconds of sleep
            int sleepTime = (int) (BASE_WAIT_TIME_MS * (Math.pow(2,i)-1) * (Math.random()));

            Thread.sleep(sleepTime);

            response = HttpHelper.sendRequest(
                    clientSocket,
                    "GET",
                    "/",
                    headers,
                    ""
            );

            updateLamportWithResponse(response);

            if (response.status.contains("200")){
                break;
            }

            System.out.println("Request Failed: " + response.status);

        }

        return response;
    }

    public static void outputStationData(String responseBody){
        Map<String, String> stationIDMap = SimpleJsonUtil.parse(responseBody);

        // try push previous data out of sight
        System.out.print("\n\n\n\n\n\n\n\n\n\n\n\n");
        for (Map.Entry<String, String> stationEntry : stationIDMap.entrySet()){

            System.out.println(stationEntry.getKey() + ":");

            Map<String, String> stationData = SimpleJsonUtil.parse(stationEntry.getValue());
            for (Map.Entry<String, String> dataEntry : stationData.entrySet()) {
                System.out.println("        " + dataEntry.getKey() + ": " + dataEntry.getValue());
            }
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("No command-line arguments provided.");
            System.out.println("Must provide URL and optionally provide a StationID");
            return;
        }

        String[] urlSplit = args[0].split(":");
        String host = urlSplit[0];
        int port = Integer.parseInt(urlSplit[1]);

        GetClient client = new GetClient();
        try {
            client.startConnection(host, port);
            while (true) {
                String stationId = null;
                if (args.length == 2) {
                    stationId = args[1];
                }
                HttpHelper.Response response = null;
                try {
                    response = client.requestStationData(stationId);
                } catch (IOException e) {
                    System.err.println("Lost connection to server. Attempting to reconnect...");
                    client.stopConnection(); // ensure old socket is closed

                    client.connectWithRetry(host, port);
                }

                if (response != null && response.status.contains("200")){
                    outputStationData(response.body);
                    // wait two seconds
                    Thread.sleep(2000);
                } else {
                    System.out.println("Failed to make request retrying...");
                }
            }
        } catch (InterruptedException e ){
            System.err.println("Client has been terminated");
        } finally {
            client.stopConnection();
        }
    }
}