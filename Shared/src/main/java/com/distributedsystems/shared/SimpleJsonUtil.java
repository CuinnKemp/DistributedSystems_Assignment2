package com.distributedsystems.shared;

import java.util.*;

/**
 * Improved simple JSON parser for flat or nested JSON values (nested as strings)
 */
public class SimpleJsonUtil {
    public static Map<String, String> parse(String json) {
        Map<String, String> map = new HashMap<>();
        json = json.trim();
        if (!json.startsWith("{") || !json.endsWith("}")) {
            return map;
        }
        json = json.substring(1, json.length() - 1).trim(); // remove outer braces

        int braceCount = 0;
        boolean inQuotes = false;
        StringBuilder key = new StringBuilder();
        StringBuilder value = new StringBuilder();
        String currentKey = null;

        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);

            if (c == '"' && (i == 0 || json.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes;
            }

            if (!inQuotes) {
                if (c == '{') braceCount++;
                if (c == '}') braceCount--;
                if (c == ':' && currentKey == null) {
                    currentKey = key.toString().trim().replaceAll("^\"|\"$", "");
                    key.setLength(0);
                    continue;
                }
                if (c == ',' && braceCount == 0) {
                    String val = value.toString().trim().replaceAll("^\"|\"$", "");
                    map.put(currentKey, val);
                    currentKey = null;
                    value.setLength(0);
                    continue;
                }
            }

            if (currentKey == null) {
                key.append(c);
            } else {
                value.append(c);
            }
        }

        if (currentKey != null) {
            String val = value.toString().trim().replaceAll("^\"|\"$", "");
            map.put(currentKey, val);
        }

        return map;
    }

    /**
     * Converts a Map<String,String> back into a JSON string.
     */
    public static String stringify(Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> e = it.next();
            sb.append("\"").append(e.getKey()).append("\":");
            String val = e.getValue();
            // if the value looks like JSON, leave it as-is; otherwise quote it
            if (val.startsWith("{") && val.endsWith("}")) {
                sb.append(val);
            } else {
                sb.append("\"").append(val).append("\"");
            }
            if (it.hasNext()) sb.append(",");
        }
        sb.append("}");
        return sb.toString();
    }
}
