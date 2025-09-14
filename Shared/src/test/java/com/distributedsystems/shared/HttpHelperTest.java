package com.distributedsystems.shared;

import org.junit.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.*;

public class HttpHelperTest {

    @Test
    public void testReadResponse() throws IOException {
        String httpResponse =
                "HTTP/1.1 200 OK\r\n" +
                        "Content-Length: 11\r\n" +
                        "Content-Type: text/plain\r\n" +
                        "\r\n" +
                        "Hello World";

        BufferedReader in = new BufferedReader(new StringReader(httpResponse));
        HttpHelper.Response res = HttpHelper.readResponse(in);

        assertEquals("HTTP/1.1", res.version);
        assertEquals("200 OK", res.status);
        assertEquals("11", res.headers.get("Content-Length"));
        assertEquals("Hello World", res.body);
    }

    @Test
    public void testReadRequest() throws IOException {
        String httpRequest =
                "POST /submit HTTP/1.1\r\n" +
                        "Host: localhost\r\n" +
                        "Content-Length: 5\r\n" +
                        "\r\n" +
                        "Hello";

        BufferedReader in = new BufferedReader(new StringReader(httpRequest));
        HttpHelper.Request req = HttpHelper.readRequest(in);

        assertEquals("POST", req.method);
        assertEquals("/submit", req.path);
        assertEquals("HTTP/1.1", req.version);
        assertEquals("localhost", req.headers.get("Host"));
        assertEquals("Hello", req.body);
    }

    @Test
    public void testSendResponse() throws IOException {
        StringWriter sw = new StringWriter();
        PrintWriter out = new PrintWriter(sw);

        HttpHelper.sendResponse(out, "200 OK", 5, "{\"msg\":\"hi\"}");

        String result = sw.toString();
        assertTrue(result.contains("HTTP/1.1 200 OK"));
        assertTrue(result.contains("X-Lamport-Clock: 5"));
        assertTrue(result.contains("{\"msg\":\"hi\"}"));
    }

    @Test
    public void testSendRequestEndToEnd() throws Exception {
        // Start a simple server socket in another thread
        ServerSocket serverSocket = new ServerSocket(0); // bind to any free port
        int port = serverSocket.getLocalPort();

        Thread serverThread = new Thread(() -> {
            try (Socket client = serverSocket.accept()) {
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(
                        new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8), true);

                // Read request line
                String line;
                while ((line = in.readLine()) != null && !line.isEmpty()) {
                    // consume headers until blank line
                }
                // Send a fixed response
                out.println("HTTP/1.1 200 OK");
                out.println("Content-Length: 2");
                out.println();
                out.write("OK");
                out.flush();
            } catch (IOException ignored) {}
        });
        serverThread.start();

        // Client sends request
        try (Socket socket = new Socket("localhost", port)) {
            HttpHelper.Response res = HttpHelper.sendRequest(socket,
                    "GET", "/hello", null, "");

            assertEquals("HTTP/1.1", res.version);
            assertEquals("200 OK", res.status);
            assertEquals("OK", res.body);
        }

        serverSocket.close();
    }
}
