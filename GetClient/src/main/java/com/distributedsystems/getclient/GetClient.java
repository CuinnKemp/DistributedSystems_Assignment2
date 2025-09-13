package com.distributedsystems.getclient;

import com.distributedsystems.shared.HttpHelper;
import com.distributedsystems.shared.LamportClock;
import com.distributedsystems.shared.SimpleJsonUtil;

import java.net.*;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class GetClient {
    private Socket clientSocket;
    private final LamportClock clock = new LamportClock();

    private static final int BASE_WAIT_TIME_MS = 10;
    private static final int MAX_REQUEST_ATTEMPTS = 10;

    /**
     * Attempts to connect to the server with unlimited retries
     *      - client will attempt to connect to server
     *      - if successful output the success!
     *      - if unsuccessful we sleep inline with exponential backoff + jitter same as requests.
     *
     * @param ip The ip we are connecting too
     * @param port The port we are connecting too
     * @throws InterruptedException if client is interrupted we pass the interruption down to main.
     */
    private void connectWithRetry(String ip, int port) throws InterruptedException {
        int attempts = 0;
        while (true) {
            try {
                this.clientSocket = new Socket(ip, port);
                updateClockViaRequest();
                System.out.println("Connected to server at " + ip + ":" + port);
                return;
            } catch (IOException e) {
                attempts++;
                int sleepTime = (int) (BASE_WAIT_TIME_MS * (Math.pow(2, Math.min(attempts, MAX_REQUEST_ATTEMPTS)) - 1) * Math.random());
                System.err.println("Connection failed (attempt " + attempts + "). Retrying in " + sleepTime + "ms...");
                Thread.sleep(sleepTime);
            }
        }
    }

    public void startConnection(String ip, int port) throws InterruptedException {
        connectWithRetry(ip, port);
    }

    public void updateClockViaRequest() throws InterruptedException, IOException {
        HttpHelper.Response response = null;
        for (int i = 0; i < MAX_REQUEST_ATTEMPTS; i++) {
            int sleepTime = (int) (BASE_WAIT_TIME_MS * (Math.pow(2,i)-1) * (Math.random()));

            Thread.sleep(sleepTime);

            response = HttpHelper.sendRequest(
                    clientSocket,
                    "GET",
                    "/lamport",
                    null,
                    ""
            );
            if (response.status.contains("200")){
                break;
            }
        }

        if (response.status.contains("200")){
            clock.update(Integer.parseInt(response.headers.get("X-Lamport-Clock")));
        }


    }

    public HttpHelper.Response requestStationData(String stationId) throws IOException, InterruptedException {
        clock.tick();
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Lamport-Clock", String.valueOf(clock.get()));
        if (stationId != null) headers.put("stationId", stationId);

        HttpHelper.Response response = null;
        for (int i = 0; i < MAX_REQUEST_ATTEMPTS; i++) {
            int sleepTime = (int) (BASE_WAIT_TIME_MS * (Math.pow(2,i)-1) * (Math.random()));

            Thread.sleep(sleepTime);

            response = HttpHelper.sendRequest(
                    clientSocket,
                    "GET",
                    "/",
                    headers,
                    ""
            );

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

    public void stopConnection() throws IOException {
        clientSocket.close();
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("No command-line arguments provided.");
            System.out.println("Must provide URL and optionally provide a StationID");
            return;
        }

        String[] urlSplit = args[0].split(":");
        String ip = urlSplit[0];
        int port = Integer.parseInt(urlSplit[1]);

        GetClient client = new GetClient();
        try {
            client.startConnection(ip, port);
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
                    try {
                        client.stopConnection(); // ensure old socket is closed
                    } catch (IOException ignored) {}

                    client.connectWithRetry(ip, port);
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