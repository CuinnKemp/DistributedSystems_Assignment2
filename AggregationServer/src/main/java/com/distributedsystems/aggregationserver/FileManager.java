package com.distributedsystems.aggregationserver;

import com.distributedsystems.shared.SimpleJsonUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * File manager: Handles file updates and ensures that read / write locks are managed correctly and ensure correct ordering
 */
public class FileManager {
    public static File DATA_DIR = new File("data");

    // Read Write Locks for stations ensures PUT GET PUT write order get cannot read until put writes, put cannot write until get reads.
    private static final ConcurrentHashMap<String, ReentrantReadWriteLock> stationLocks = new ConcurrentHashMap<>();

    // Stores when a station was last written to - if data is stale we remove the file
    private static final ConcurrentHashMap<String, Instant> stationLastWrite = new ConcurrentHashMap<>();

    /**
     * Initialises the data store folder in project root
     */
    public FileManager() {
        if (!DATA_DIR.exists() && !DATA_DIR.mkdirs()) {
            throw new RuntimeException("Failed to create data directory");
        }
    }

    public enum UpdateResult { CREATED, UPDATED, STALE }

    /**
     * Updates a station file - locks file and checks if it needs to be updated (cur lamport < new lamport)
     * Updates stationLastWrite if file is updated - used for removing stale files
     *
     * @param stationId station Id to be updated
     * @param lamport Lamport clock time to write to file
     * @param json the data to write to file
     * @return information about the update
     * @throws IOException if file write errors occur
     */
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
                    return UpdateResult.STALE;
                }
            }

            Map<String, String> wrapped = new HashMap<>();
            wrapped.put("lamport", String.valueOf(lamport));
            wrapped.put("data", SimpleJsonUtil.stringify(json));

            File tmp = new File(DATA_DIR, stationId + ".tmp");
            try (FileWriter fw = new FileWriter(tmp)) {
                fw.write(SimpleJsonUtil.stringify(wrapped));
            }

            Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            boolean isNew = !stationLastWrite.containsKey(stationId);
            stationLastWrite.put(stationId, Instant.now());
            return isNew ? UpdateResult.CREATED : UpdateResult.UPDATED;

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Reads data from a single station
     *
     * @param stationId station to read data for
     * @return a map of stationId : data as json string
     */
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
            lock.readLock().unlock();
        }
        return flatMap;
    }

    /**
     * Reads data from all stations and returns them together
     *
     * @return a map of stationId : data as json string
     */
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
                    lock.readLock().unlock();
                }
            }
        }
        return flatMap;
    }

    /**
     * Called when a server starts up and will read all exisiting files on the disc and update
     * lock and timestamp maps.
     *
     * @return the maximum lamport clock present in data
     */
    public static int reloadStationsOnStartup() {
        int maxLamport = 0;

        File[] files = DATA_DIR.listFiles((dir, name) -> name.endsWith(".json"));
        if (files == null) return 0;

        for (File file : files) {
            try {
                String content = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
                Map<String, String> wrapped = SimpleJsonUtil.parse(content);

                // stationId is filename (strip .json)
                String stationId = file.getName().replace(".json", "");

                int lamport = Integer.parseInt(wrapped.getOrDefault("lamport", "0"));
                maxLamport = Math.max(maxLamport, lamport);

                // data was wrapped as JSON string, unwrap
                String dataJson = wrapped.get("data");
                Map<String, String> ignored = SimpleJsonUtil.parse(dataJson); // test that data can be parsed

                // put into memory structures
                stationLastWrite.put(stationId, Instant.now());
                stationLocks.putIfAbsent(stationId, new ReentrantReadWriteLock());

                System.out.println("Reloaded station " + stationId + " (lamport=" + lamport + ")");
            } catch (Exception e) {
                System.err.println("Failed to reload " + file.getName() + ": " + e.getMessage());
            }
        }

        return maxLamport;
    }

    /**
     * Internal Runnable class that can be executed on a thread to remove stale data
     */
    public static class StaleDataRemover implements Runnable {
        int dataExpirationSeconds;

        /**
         * Creates a StaleDataRemoverObject that removes data after it has not been updated after time
         * @param dataExpirationSeconds the time that data has to be untouched before it becomes stale
         */
        StaleDataRemover(int dataExpirationSeconds){
            this.dataExpirationSeconds = dataExpirationSeconds;
        }

        private void removeStaleData(){
            Instant removeOlderThan = Instant.now().minusSeconds(dataExpirationSeconds);
            for (Map.Entry<String, Instant> entry : stationLastWrite.entrySet()){
                stationLocks.get(entry.getKey()).writeLock().lock();
                if (entry.getValue().isBefore(removeOlderThan)){
                    File file = new File(DATA_DIR, entry.getKey() + ".json");
                    if (file.exists()){
                        if (file.delete()){
                            System.out.println("Data for station: " + entry.getKey() + " expired");
                        } else {
                            System.out.println("Failed to delete data for station: " + entry.getKey());
                        }
                    }
                }
                stationLocks.get(entry.getKey()).writeLock().unlock();
            }
        }

        @Override
        public void run() {
            removeStaleData();
        }
    }

}
