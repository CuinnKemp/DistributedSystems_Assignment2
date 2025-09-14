package com.distributedsystems.shared;

import java.io.IOException;
import java.net.Socket;

public class AggregationServerClient {
    protected Socket clientSocket;
    protected final LamportClock clock = new LamportClock();

    protected static final int BASE_WAIT_TIME_MS = 10;
    protected static final int MAX_REQUEST_ATTEMPTS = 10;

    /**
     * Attempts to connect to the server with unlimited retries
     *      - client will attempt to connect to server
     *      - if successful output the success!
     *      - if unsuccessful we sleep inline with exponential backoff + jitter same as requests.
     *
     * @param host The host we are connecting too
     * @param port The port we are connecting too
     * @throws InterruptedException if client is interrupted we pass the interruption down to main.
     */
    protected void connectWithRetry(String host, int port) throws InterruptedException {
        int attempts = 0;
        while (true) {
            clock.tick();
            try {
                this.clientSocket = new Socket(host, port);
                updateClockViaRequest();
                System.out.println("Connected to server at " + host + ":" + port);
                return;
            } catch (IOException e) {
                attempts++;
                int sleepTime = (int) (BASE_WAIT_TIME_MS * (Math.pow(2, Math.min(attempts, MAX_REQUEST_ATTEMPTS)) - 1) * Math.random());
                System.err.println("Connection failed (attempt " + attempts + "). Retrying in " + sleepTime + "ms...");
                Thread.sleep(sleepTime);
            }
        }
    }

    /**
     * Requests the Servers Current Lamport Clock
     *
     * @throws InterruptedException if interrupted
     * @throws IOException if Socket fails or disconnects
     */
    public void updateClockViaRequest() throws InterruptedException, IOException {
        HttpHelper.Response response = null;
        for (int i = 0; i < MAX_REQUEST_ATTEMPTS; i++) {
            clock.tick();

            // exponential back off
            int sleepTime = (int) (BASE_WAIT_TIME_MS * (Math.pow(2,i)-1) * (Math.random()));
            Thread.sleep(sleepTime);

            response = HttpHelper.sendRequest(
                    clientSocket,
                    "GET",
                    "/lamport",
                    null,
                    ""
            );

            updateLamportWithResponse(response);

            // break on successful response
            if (response.status.contains("200")){
                break;
            }
        }
    }

    /**
     * starts connection (wrapper for connectWithRetry for initial start of server)
     * @param host host name
     * @param port port to connect to on host
     * @throws InterruptedException if interrupted while sleeping
     */
    public void startConnection(String host, int port) throws InterruptedException {
        connectWithRetry(host, port);
    }

    /**
     * Helper function that tries to update the lamport clock using a request
     * @param response The response sent from the Aggregation Server
     */
    protected void updateLamportWithResponse(HttpHelper.Response response){
        if (response != null && response.headers.containsKey("X-Lamport-Clock")){
            try {
                clock.update(Integer.parseInt(response.headers.get("X-Lamport-Clock")));
            } catch (NumberFormatException ignored) {}
        }
    }

    /**
     * Attempts to stop the connection - can be used without checking connection status etc.
     */
    public void stopConnection() {
        try {
            clientSocket.close();
        } catch (IOException ignored){
            // already closed swallowing error
        }
    }
}
