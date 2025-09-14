package com.distributedsystems.aggregationserver;

import com.distributedsystems.shared.HttpHelper;
import com.distributedsystems.shared.LamportClock;
import com.distributedsystems.shared.SimpleJsonUtil;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * Client Handler: Allows Aggregation Server to start threads to handle clients requests.
 */
public class ClientHandler implements Runnable {
    private final Socket socket;
    private final LamportClock clock;
    private static final FileManager fileManager = new FileManager();

    /**
     * Creates an instance of the ClientHandler with provided socket and a reference to the server lamport clock
     * @param socket the client socket that spawned this thread
     * @param clock the server lamport clock
     */
    public ClientHandler(Socket socket, LamportClock clock) {
        this.socket = socket;
        this.clock = clock;
    }

    /**
     * Executed upon thread start - receives client requests and handles them while the socket is connected
     */
    @Override
    public void run() {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            while (socket.isConnected()) {
                HttpHelper.Request req = HttpHelper.readRequest(in);
                if (req == null) return;

                int clientClock = Integer.parseInt(req.headers.getOrDefault("X-Lamport-Clock", "0"));
                clock.update(clientClock);

                String method = req.method.toUpperCase();
                switch (method) {
                    case "PUT":
                        handlePut(req, out);
                        break;
                    case "GET":
                        handleGet(req, out);
                        break;
                    default:
                        HttpHelper.sendResponse(out, "400 Bad Request", clock.get(),
                                "Unsupported method: " + req.method);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Handles a put request - extracts necessary data and calls the FileManager to update the specified file
     *                          Sends 201 if a new file was created,
     *                          200 if a file was updated or if a stale update was ignored
     *                          400 if the request is missing a body or a station id
     *
     * @param req the http request sent from the content server
     * @param out the output stream to respond on
     */
    private void handlePut(HttpHelper.Request req, PrintWriter out) {
        if (req.body == null || req.body.isEmpty()) {
            HttpHelper.sendResponse(out, "400 Bad Request", clock.get(), "No Body");
            return;
        }
        try {
            Map<String, String> json = SimpleJsonUtil.parse(req.body);
            String stationId = json.get("id");
            System.out.println("Handling PUT from " + stationId);
            int requestLamport = Integer.parseInt(req.headers.getOrDefault("X-Lamport-Clock", "0"));
            clock.update(requestLamport);

            if (stationId == null) {
                HttpHelper.sendResponse(out, "400 Bad Request", clock.get(), "Missing station ID");
                return;
            }

            // update file using lamport from request - ensures most recent update is always the update available
            FileManager.UpdateResult result = fileManager.updateStation(stationId, requestLamport, json);

            switch (result) {
                case CREATED:
                    HttpHelper.sendResponse(out, "201 Created", clock.get(),
                            "New station " + stationId + " created");
                    break;
                case UPDATED:
                    HttpHelper.sendResponse(out, "200 OK", clock.get(),
                            "Updated station " + stationId);
                    break;
                case STALE:
                    HttpHelper.sendResponse(out, "200 OK", clock.get(),
                            "Stale update ignored");
                    break;
            }

        } catch (Exception e) {
            HttpHelper.sendResponse(out, "500 Internal Server Error", clock.get(), "");
        }
    }

    /**
     * Handles Get Requests:
     *      - getting lamport clock using "/lamport"
     *      - default path i.e. "/" sends all data if no stationId is provided
     *      - default path i.e. "/" sends specific station data if id provided
     *      * both default path "/" return a json with station ID followed by the associated data
     *
     * @param req request sent to server
     * @param out the output stream to send response on
     */
    private void handleGet(HttpHelper.Request req, PrintWriter out) {
        clock.tick();
        Map<String, String> body = new HashMap<>();
        switch (req.path){
            case "/lamport":
                body.put("lamport", String.valueOf(clock.get()));
                HttpHelper.sendResponse(out, "200 OK", clock.get(), SimpleJsonUtil.stringify(body));
                break;
            case "/":
                if (req.headers.containsKey("stationId")){
                    Map<String, String> singleStation = fileManager.readSingleStation(req.headers.get("stationId"));
                    HttpHelper.sendResponse(out, "200 OK", clock.get(), SimpleJsonUtil.stringify(singleStation));
                    break;
                }

                Map<String, String> allStations = fileManager.readAllStations();
                HttpHelper.sendResponse(out, "200 OK", clock.get(), SimpleJsonUtil.stringify(allStations));
                break;
            default:
                HttpHelper.sendResponse(out, "400 Not Found", clock.get(), "{\"reason\": \"requested path is not implemented\"}");
        }
    }
}