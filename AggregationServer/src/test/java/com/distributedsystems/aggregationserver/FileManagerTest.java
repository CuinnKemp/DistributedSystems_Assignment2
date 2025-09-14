package com.distributedsystems.aggregationserver;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FileManagerTest {

    @TempDir
    File tempDir;

    FileManager fileManager;

    @BeforeEach
    void setup() {
        // Point the FileManager.DATA_DIR to our temp dir using reflection
        try {
            var field = FileManager.class.getDeclaredField("DATA_DIR");
            field.setAccessible(true);
            field.set(null, tempDir);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        fileManager = new FileManager();
    }

    @Test
    void testUpdateStationCreated() throws IOException {
        Map<String, String> data = new HashMap<>();
        data.put("key", "value");

        FileManager.UpdateResult result =
                fileManager.updateStation("32432rwe243er", 1, data);

        assertEquals(FileManager.UpdateResult.CREATED, result);
        assertTrue(new File(tempDir, "32432rwe243er.json").exists());
    }

    @Test
    void testUpdateStationUpdated() throws IOException {
        Map<String, String> data = new HashMap<>();
        data.put("key", "v1");

        fileManager.updateStation("43253252feew", 1, data);

        data.put("key", "v2");
        FileManager.UpdateResult result =
                fileManager.updateStation("43253252feew", 2, data);

        assertEquals(FileManager.UpdateResult.UPDATED, result);
    }

    @Test
    void testUpdateStationStale() throws IOException {
        Map<String, String> data = new HashMap<>();
        data.put("key", "v1");

        fileManager.updateStation("station1", 5, data);

        data.put("key", "old");
        FileManager.UpdateResult result =
                fileManager.updateStation("station1", 3, data);

        assertEquals(FileManager.UpdateResult.STALE, result);
    }

    @Test
    void testReadSingleStation() throws IOException {
        Map<String, String> data = new HashMap<>();
        data.put("temp", "20");

        fileManager.updateStation("station1", 1, data);

        Map<String, String> read = fileManager.readSingleStation("station1");
        assertEquals("{\"temp\":\"20\"}", read.get("station1")); // raw JSON string
    }

    @Test
    void testReadAllStations() throws IOException {
        Map<String, String> d1 = new HashMap<>();
        d1.put("a", "1");
        Map<String, String> d2 = new HashMap<>();
        d2.put("b", "2");

        fileManager.updateStation("s1", 1, d1);
        fileManager.updateStation("s2", 1, d2);

        Map<String, String> all = fileManager.readAllStations();
        assertEquals(2, all.size());
        assertTrue(all.get("s1").contains("\"a\":\"1\""));
        assertTrue(all.get("s2").contains("\"b\":\"2\""));
    }

    @Test
    void testReloadStationsOnStartup() throws IOException {
        Map<String, String> d1 = new HashMap<>();
        d1.put("x", "100");
        fileManager.updateStation("reloadMe", 42, d1);

        int maxLamport = FileManager.reloadStationsOnStartup();
        assertEquals(42, maxLamport);
    }

    @Test
    void testStaleDataRemoverDeletesOldFile() throws IOException {
        Map<String, String> d1 = new HashMap<>();
        d1.put("k", "v");

        fileManager.updateStation("oldStation", 1, d1);

        // Force stationLastWrite to be very old
        try {
            var field = FileManager.class.getDeclaredField("stationLastWrite");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, Instant> stationLastWrite = (Map<String, Instant>) field.get(null);
            stationLastWrite.put("oldStation", Instant.now().minusSeconds(9999));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        FileManager.StaleDataRemover remover = new FileManager.StaleDataRemover(1);
        remover.run();

        assertFalse(new File(tempDir, "oldStation.json").exists());
    }
}
