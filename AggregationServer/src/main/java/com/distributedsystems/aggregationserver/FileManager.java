package com.distributedsystems.aggregationserver;

import com.distributedsystems.shared.SimpleJsonUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * File manager: Handles file updates and ensures that read / write locks are managed correctly and ensure correct ordering
 */
public class FileManager {
    private static final File DATA_DIR = new File("data");

    // Read Write Locks for stations ensures PUT GET PUT write order get cannot read until put writes, put cannot write until get reads.
    private static final ConcurrentHashMap<String, ReentrantReadWriteLock> stationLocks = new ConcurrentHashMap<>();

    // Stores when a station was last written to - if data is stale we remove the file
    private static final ConcurrentHashMap<String, Instant> stationLastWrite = new ConcurrentHashMap<>();

    public FileManager() {
        if (!DATA_DIR.exists() && !DATA_DIR.mkdirs()) {
            throw new RuntimeException("Failed to create data directory");
        }
    }

    public enum UpdateResult { CREATED, UPDATED, STALE }

    public UpdateResult updateStation(String stationId, int lamport, Map<String, String> json) throws IOException {
        ReentrantReadWriteLock lock = stationLocks.computeIfAbsent(stationId, k -> new ReentrantReadWriteLock());
        lock.writeLock().lock();
        try {
            File file = new File(DATA_DIR, stationId + ".json");
            boolean createNewFile = !file.exists();

            if (!createNewFile) {
                String existing = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
                Map<String, String> existingJson = SimpleJsonUtil.parse(existing);
                int oldLamport = Integer.parseInt(existingJson.getOrDefault("lamport", "0"));

                if (lamport <= oldLamport) {
                    return UpdateResult.STALE; // reject stale update
                }
            }

            Map<String, String> wrapped = new HashMap<>();
            wrapped.put("lamport", String.valueOf(lamport));
            wrapped.put("data", SimpleJsonUtil.stringify(json));

            File tmp = new File(DATA_DIR, stationId + ".tmp");
            try (FileWriter fw = new FileWriter(tmp)) {
                fw.write(SimpleJsonUtil.stringify(wrapped));
            }
            if (!tmp.renameTo(file)) {
                throw new IOException("Failed to rename temp file to " + file.getName());
            }

            boolean isNew = stationLastWrite.containsKey(stationId);
            stationLastWrite.put(stationId, Instant.now());
            return isNew ? UpdateResult.CREATED : UpdateResult.UPDATED;

        } finally {
            lock.writeLock().unlock();
        }
    }

    public Map<String, String> readSingleStation(String stationId) {
        Map<String, String> flatMap = new HashMap<>();
        File file = new File(DATA_DIR, stationId + ".json");
        if (!file.exists()) {
            return flatMap;
        }
        ReentrantReadWriteLock lock = stationLocks.computeIfAbsent(stationId, k -> new ReentrantReadWriteLock());
        lock.readLock().lock();
        try {
            String json = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
            Map<String, String> content = SimpleJsonUtil.parse(json);
            flatMap.put(stationId, content.get("data"));
        } catch (IOException ignored) {}
        finally {
            lock.readLock().lock();
        }
        return flatMap;
    }

    public Map<String, String> readAllStations() {
        Map<String, String> flatMap = new HashMap<>();
        File[] files = DATA_DIR.listFiles((dir, name) -> name.endsWith(".json"));
        if (files != null) {
            for (File f : files) {
                String stationId = f.getName().replace(".json", "");

                ReentrantReadWriteLock lock = stationLocks.computeIfAbsent(stationId, k -> new ReentrantReadWriteLock());
                lock.readLock().lock();
                try {
                    Map<String, String> content = SimpleJsonUtil.parse(new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8));
                    flatMap.put(stationId, content.get("data"));

                } catch (IOException ignored) {}
                finally {
                    lock.readLock().lock();
                }
            }
        }
        return flatMap;
    }
}
