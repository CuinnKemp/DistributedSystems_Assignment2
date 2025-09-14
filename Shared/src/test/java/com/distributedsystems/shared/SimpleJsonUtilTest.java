package com.distributedsystems.shared;

import org.junit.Test;

import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.*;

public class SimpleJsonUtilTest {

    @Test
    public void testParseFlatJson() {
        String json = "{\"a\":\"1\",\"b\":\"2\"}";
        Map<String, String> map = SimpleJsonUtil.parse(json);

        assertEquals(2, map.size());
        assertEquals("1", map.get("a"));
        assertEquals("2", map.get("b"));
    }

    @Test
    public void testParseNestedJsonString() {
        String nested = "{\"x\":\"10\",\"y\":\"20\"}";
        // Escape quotes, because SimpleJsonUtil.parse expects JSON string format
        String json = "{\"nested\":\"" + nested.replace("\"","\\\"") + "\",\"other\":\"val\"}";

        Map<String, String> map = SimpleJsonUtil.parse(json);

        assertEquals(2, map.size());
        // Nested JSON is preserved as string with escaped quotes
        assertEquals(nested.replace("\"","\\\""), map.get("nested"));
        assertEquals("val", map.get("other"));
    }


    @Test
    public void testParseEmptyJson() {
        String json = "{}";
        Map<String, String> map = SimpleJsonUtil.parse(json);

        assertTrue(map.isEmpty());
    }

    @Test
    public void testParseInvalidJson() {
        String json = "not a json";
        Map<String, String> map = SimpleJsonUtil.parse(json);

        assertTrue(map.isEmpty());
    }

    @Test
    public void testStringifyFlatMap() {
        Map<String, String> map = new HashMap<>();
        map.put("a", "1");
        map.put("b", "2");

        String json = SimpleJsonUtil.stringify(map);
        assertTrue(json.contains("\"a\":\"1\""));
        assertTrue(json.contains("\"b\":\"2\""));
    }

    @Test
    public void testStringifyWithNestedJsonString() {
        Map<String, String> map = new HashMap<>();
        map.put("nested", "{\"x\":\"10\",\"y\":\"20\"}");
        map.put("other", "val");

        String json = SimpleJsonUtil.stringify(map);
        assertTrue(json.contains("\"nested\":{\"x\":\"10\",\"y\":\"20\"}"));
        assertTrue(json.contains("\"other\":\"val\""));
    }

    @Test
    public void testParseAndStringifyRoundTrip() {
        Map<String, String> nestedMap = new HashMap<>();
        nestedMap.put("x", "10");
        Map<String, String> map = new HashMap<>();
        map.put("nested", SimpleJsonUtil.stringify(nestedMap));
        map.put("y", "20");
        String output = SimpleJsonUtil.stringify(map);

        assertTrue(output.contains("\"nested\":{\"x\":\"10\"}"));
        assertTrue(output.contains("\"y\":\"20\""));

        map = SimpleJsonUtil.parse(output);
        assertEquals(map.get("nested"), SimpleJsonUtil.stringify(nestedMap));
        assertEquals("20", map.get("y"));
    }
}
