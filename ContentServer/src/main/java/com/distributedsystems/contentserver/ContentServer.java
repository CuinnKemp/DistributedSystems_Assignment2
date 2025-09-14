package com.distributedsystems.contentserver;

import com.distributedsystems.shared.AggregationServerClient;
import com.distributedsystems.shared.HttpHelper;
import com.distributedsystems.shared.SimpleJsonUtil;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


/**
 * ContentServer sends data stored in specified file to an aggregation server
 */
public class ContentServer extends AggregationServerClient {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java ContentServer <server:port | http://host:port[/path]> <local-data-file>");
            System.exit(1);
        }

        String[] urlSplit = args[0].split(":");
        String host = urlSplit[0];
        int port = Integer.parseInt(urlSplit[1]);

        String filePath = args[1];

        ContentServer contentServer = new ContentServer();
        try {
            contentServer.startConnection(host, port);
            while (true) {

                File dataFile = new File(filePath);
                if (!dataFile.exists() || !dataFile.isFile()) {
                    System.err.println("Data file not found: " + filePath);
                    System.exit(1);
                }

                HttpHelper.Response response = null;
                try {
                    response = contentServer.sendData(dataFile);
                } catch (IOException e) {
                    System.err.println("Lost connection to server. Attempting to reconnect...");
                    // close old socket
                    contentServer.stopConnection();
                    contentServer.connectWithRetry(host, port);
                }

                if (response != null && response.status.contains("201")){
                    System.out.println("Successfully updated data");
                    // would probably wait until another thread sent a message notifying of file update
                } else if (response != null && response.status.contains("200")){
                    System.out.println("Successfully updated data");
                    // would probably wait until another thread sent a message notifying of file update
                } else {
                    System.out.println("Failed to make request retrying...");
                }
                Thread.sleep(5000);
            }
        } catch (InterruptedException e ){
            System.err.println("Client has been terminated");
        } finally {
            contentServer.stopConnection();
        }
    }

    /**
     * Read the data file, build JSON, and send a single PUT to the aggregation server.
     */
    public HttpHelper.Response sendData(File dataFile) throws IOException {
        Map<String, String> data = readKeyValueFile(dataFile);
        if (data.isEmpty()) {
            System.err.println("No data parsed from file; nothing to send.");
            throw new FileNotFoundException("Data File is empty or not found");
        }

        // Ensure there is an id if possible (aggregation server expects station id)
        if (!data.containsKey("id")) {
            System.err.println("Warning: data does not contain an 'id' key. Aggregation server may reject or treat differently.");
        }

        System.out.println("sending data");
        String jsonBody = SimpleJsonUtil.stringify(data);

        HttpHelper.Response response = null;
        for (int i = 0; i < MAX_REQUEST_ATTEMPTS; i++) {
            System.out.println("sending data");
            clock.tick(); // advance before sending
            Map<String, String> headers = new HashMap<>();
            headers.put("X-Lamport-Clock", String.valueOf(clock.get()));
            headers.put("Content-Type", "application/json");

            response = HttpHelper.sendRequest(
                    clientSocket,
                    "PUT",
                    "/",
                    headers,
                    jsonBody
            );

            updateLamportWithResponse(response);

            if (response.status.contains("200") || response.status.contains("201")){
                break;
            }

            System.out.println("Request Failed: " + response.status);

        }

        return response;
    }

    /**
     * File parser split lines based on :, = or just use line as key
     */
    private static Map<String, String> readKeyValueFile(File file) throws IOException {
        Map<String, String> map = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue; // skip potential empty lines

                String key;
                String value;

                int idx;
                if ((idx = line.indexOf(':')) >= 0) { // split on ":"
                    key = line.substring(0, idx).trim();
                    value = line.substring(idx + 1).trim();
                } else if ((idx = line.indexOf('=')) >= 0) { // split on "="
                    key = line.substring(0, idx).trim();
                    value = line.substring(idx + 1).trim();
                } else {
                    // no separator: treat whole line as a key with empty value
                    key = line;
                    value = "";
                }

                if (!key.isEmpty()) {
                    map.put(key, value);
                }
            }
        }
        return map;
    }
}
