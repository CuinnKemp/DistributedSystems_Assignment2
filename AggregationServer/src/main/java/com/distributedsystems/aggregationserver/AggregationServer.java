package com.distributedsystems.aggregationserver;

import com.distributedsystems.shared.LamportClock;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;

public class AggregationServer {
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

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Aggregation Server started on port " + port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientPool.submit(new ClientHandler(clientSocket, clock));
            }
        }
    }
}
