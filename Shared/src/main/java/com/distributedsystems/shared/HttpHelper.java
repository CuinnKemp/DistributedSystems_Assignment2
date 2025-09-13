package com.distributedsystems.shared;

import java.io.*;
import java.net.Socket;
import java.util.*;

/**
 * HTTP Helper class that contains utility functions and classes
 */
public class HttpHelper {
    public static class Request {
        public String method;
        public String path;
        public String version;
        public Map<String, String> headers = new HashMap<>();
        public String body;

        @Override
        public String toString() {
            return method + " " + path + " " + version + "\nHeaders: " + headers + "\nBody: " + body;
        }
    }

    public static class Response {
        public String version;
        public String status;
        public Map<String, String> headers = new HashMap<>();
        public String body;

        @Override
        public String toString() {
            return version + " " + status + "\nHeaders: " + headers + "\nBody: " + body;
        }
    }

    public static Response readResponse(BufferedReader in) throws IOException {
        Response res = new Response();
        String statusLine = in.readLine();
        if (statusLine == null || statusLine.isEmpty()) {
            return null; // server closed
        }

        String[] parts = statusLine.split(" ", 3);
        if (parts.length >= 2) {
            res.version = parts[0];
            res.status = parts[1] + (parts.length == 3 ? " " + parts[2] : "");
        }
        String line;
        int contentLength = 0;
        while (!(line = in.readLine()).isEmpty()) {
            int idx = line.indexOf(":");
            if (idx > 0) {
                String key = line.substring(0, idx).trim();
                String value = line.substring(idx + 1).trim();
                res.headers.put(key, value);
                if (key.equalsIgnoreCase("Content-Length")) {
                    contentLength = Integer.parseInt(value);
                }
            }
        }

        // Body
        if (contentLength > 0) {
            char[] buf = new char[contentLength];
            in.read(buf, 0, contentLength);
            res.body = new String(buf);
        } else {
            res.body = "";
        }

        return res;
    }


    public static Request readRequest(BufferedReader in) throws IOException {
        Request req = new Request();

        // Request line: METHOD path version
        String requestLine = in.readLine();
        if (requestLine == null || requestLine.isEmpty()) {
            return null; // client closed connection
        }
        String[] parts = requestLine.split(" ");
        if (parts.length >= 3) {
            req.method = parts[0];
            req.path = parts[1];
            req.version = parts[2];
        }

        // Headers
        String line;
        int contentLength = 0;
        while (!(line = in.readLine()).isEmpty()) {
            int idx = line.indexOf(":");
            if (idx > 0) {
                String key = line.substring(0, idx).trim();
                String value = line.substring(idx + 1).trim();
                req.headers.put(key, value);
                if (key.equalsIgnoreCase("Content-Length")) {
                    contentLength = Integer.parseInt(value);
                }
            }
        }

        if (contentLength > 0) {
            char[] buf = new char[contentLength];
            in.read(buf, 0, contentLength);
            req.body = new String(buf);
        } else {
            req.body = "";
        }

        return req;
    }

    public static void sendResponse(PrintWriter out, String status, int lamport,  String body) {
        out.println("HTTP/1.1 " + status);
        out.println("Content-Type: application/json");
        out.println("X-Lamport-Clock: " + lamport);
        out.println("Content-Length: " + body.length());
        out.println();
        out.print(body);
        out.flush();
    }

    public static Response sendRequest(Socket socket, String method, String path, Map<String, String> headers, String body) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        if (body == null) body = "";
        if (headers == null) headers = new HashMap<>();

        out.println(method + " " + path + " HTTP/1.1");

        if (!headers.containsKey("Host")) {
            headers.put("Host", socket.getInetAddress().getHostName());
        }
        if (!headers.containsKey("Content-Length")) {
            headers.put("Content-Length", String.valueOf(body.length()));
        }

        // Write headers
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            out.println(entry.getKey() + ": " + entry.getValue());
        }
        out.println();

        // Write body if present
        if (!body.isEmpty()) {
            out.print(body);
        }
        out.flush();

        return readResponse(in);
    }
}
