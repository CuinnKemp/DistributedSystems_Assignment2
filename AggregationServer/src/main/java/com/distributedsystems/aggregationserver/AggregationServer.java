package com.distributedsystems.aggregationserver;

import com.distributedsystems.shared.LamportClock;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;

public class AggregationServer {
    private static final int SECONDS_UNTIL_STALE = 30;

    private ServerSocket serverSocket;
    private final int port;
    private final LamportClock clock = new LamportClock();
    private final ExecutorService clientPool = Executors.newCachedThreadPool();

    public AggregationServer(int port) {
        this.port = port;
    }


    public static void main(String[] args) throws IOException {
        int port = 4567;
        if (args.length > 0) port = Integer.parseInt(args[0]);
        new AggregationServer(port).start();
    }

    public void stop() {
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}

        clientPool.shutdownNow();
    }

    /**
     * starts up an aggregation server and starts handling requests
     * @throws IOException if the socket fails
     */
    public void start() throws IOException {
        int maxLamport = FileManager.reloadStationsOnStartup();
        clock.update(maxLamport);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(new FileManager.StaleDataRemover(SECONDS_UNTIL_STALE), 5, 5, TimeUnit.SECONDS);

        serverSocket = new ServerSocket(port);
        System.out.println("Aggregation Server started on port " + port);

        while (!serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                clientPool.submit(new ClientHandler(clientSocket, clock));
            } catch (IOException e) {
                if (serverSocket.isClosed()) {
                    System.out.println("Server stopped.");
                    break;
                }
                throw e;
            }
        }
    }

}
