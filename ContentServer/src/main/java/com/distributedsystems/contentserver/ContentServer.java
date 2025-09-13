package com.distributedsystems.contentserver;

import com.distributedsystems.shared.HttpHelper;
import com.distributedsystems.shared.SimpleJsonUtil;
import com.distributedsystems.shared.LamportClock;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class ContentServer {

    private final String host;
    private final int port;
    private final String path;
    private final File dataFile;
    private final LamportClock clock = new LamportClock();

    public ContentServer(String host, int port, String path, File dataFile) {
        this.host = host;
        this.port = port;
        this.path = (path == null || path.isEmpty()) ? "/" : path;
        this.dataFile = dataFile;
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java ContentServer <server:port | http://host:port[/path]> <local-data-file>");
            System.exit(1);
        }

        String[] urlSplit = args[0].split(":");
        String host = urlSplit[0];
        int port = Integer.parseInt(urlSplit[1]);

        String filePath = args[1];
        File dataFile = new File(filePath);
        if (!dataFile.exists() || !dataFile.isFile()) {
            System.err.println("Data file not found: " + filePath);
            System.exit(1);
        }

        ContentServer contentServer = new ContentServer(host, port, "/", dataFile);

        try {
            contentServer.runOnce();
        } catch (Exception e) {
            System.err.println("Error while running ContentServer: " + e.getMessage());
            e.printStackTrace();
            System.exit(2);
        }
    }

    /**
     * Read the data file, build JSON, and send a single PUT to the aggregation server.
     */
    public void runOnce() throws IOException {
        Map<String, String> data = readKeyValueFile(dataFile);
        if (data.isEmpty()) {
            System.err.println("No data parsed from file; nothing to send.");
            return;
        }

        // Ensure there is an id if possible (aggregation server expects station id)
        if (!data.containsKey("id")) {
            System.err.println("Warning: data does not contain an 'id' key. Aggregation server may reject or treat differently.");
        }

        String jsonBody = SimpleJsonUtil.stringify(data);

        clock.tick(); // advance before sending
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Lamport-Clock", String.valueOf(clock.get()));
        headers.put("Content-Type", "application/json");

        // Open a socket per the helper contract
        try (Socket socket = new Socket(host, port)) {
            HttpHelper.Response response = HttpHelper.sendRequest(
                    socket,
                    "PUT",
                    path,
                    headers,
                    jsonBody
            );

            if (response == null) {
                System.err.println("No response from server.");
                return;
            }

            System.out.println("Response status: " + response.status);
            if (response.body != null && !response.body.isEmpty()) {
                System.out.println("Response body:\n" + response.body);
            }
        }
    }

    /**
     * Very small tolerant parser for file containing key/value pairs.
     * Accepts:
     *   key:value
     *   key = value
     *   key=value
     * Lines starting with # or blank lines are ignored.
     */
    private static Map<String, String> readKeyValueFile(File file) throws IOException {
        Map<String, String> map = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                if (line.startsWith("#")) continue;

                String key = null;
                String value = null;

                int idx;
                if ((idx = line.indexOf(':')) >= 0) {
                    key = line.substring(0, idx).trim();
                    value = line.substring(idx + 1).trim();
                } else if ((idx = line.indexOf('=')) >= 0) {
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
