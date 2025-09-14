package com.distributedsystems.integration;

import com.distributedsystems.aggregationserver.AggregationServer;
import com.distributedsystems.aggregationserver.FileManager;
import com.distributedsystems.contentserver.ContentServer;
import com.distributedsystems.getclient.GetClient;
import com.distributedsystems.shared.HttpHelper;
import com.distributedsystems.shared.SimpleJsonUtil;
import org.junit.jupiter.api.*;

import java.io.*;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ServerIntegrationTest {

    private static int TEST_PORT;
    private AggregationServer aggregationServer;
    private ExecutorService serverExecutor;

    /**
     * Start the aggregation server at the start of each test
     */
    @BeforeAll
    void startAggregationServer() throws IOException {
        Path tempDataDir = Files.createTempDirectory("aggregation-data-");
        tempDataDir.toFile().deleteOnExit(); // ensure cleanup when JVM exits
        FileManager.DATA_DIR = tempDataDir.toFile();

        try (ServerSocket socket = new ServerSocket(0)) {
            TEST_PORT = socket.getLocalPort(); // OS picks a free port
        }

        aggregationServer = new AggregationServer(TEST_PORT);
        serverExecutor = Executors.newSingleThreadExecutor();
        serverExecutor.submit(() -> {
            for (int i = 0; i < 5; i++){
                try {
                    aggregationServer.start();
                    break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * Stop the server after each test
     */
    @AfterAll
    void stopServer() throws IOException {
        if (aggregationServer != null) {
            aggregationServer.stop(); // closes serverSocket
        }
        if (serverExecutor != null) {
            serverExecutor.shutdownNow();
        }

        if (FileManager.DATA_DIR.exists()) {
            Files.walk(FileManager.DATA_DIR.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete); // deletes files and subdirectories
        }
    }

    /**
     * Test that content can be sent and received by the client
     *
     * @throws IOException when socket fails
     * @throws InterruptedException when sleep interrupted
     */
    @Test
    void testContentPushAndClientRetrieve() throws IOException, InterruptedException {
        File tempData = createTempStationFile("TEST123", 25, 60);

        ContentServer contentServer = new ContentServer();
        contentServer.startConnection("localhost", TEST_PORT);
        HttpHelper.Response response = contentServer.sendData(tempData);
        contentServer.stopConnection();

        assertTrue(response.status.contains("200") || response.status.contains("201"));

        GetClient getClient = new GetClient();
        getClient.startConnection("localhost", TEST_PORT);
        HttpHelper.Response getResponse = getClient.requestStationData("TEST123");
        getClient.stopConnection();

        assertTrue(getResponse.status.contains("200"));

        Map<String, String> stationData = com.distributedsystems.shared.SimpleJsonUtil.parse(getResponse.body);
        assertEquals("25", SimpleJsonUtil.parse(stationData.get("TEST123")).get("air_temp"));
        assertEquals("60", SimpleJsonUtil.parse(stationData.get("TEST123")).get("rel_hum"));
    }

    /**
     * Test that the first put from a server gets a 201 and the second gets a 200
     *
     * @throws IOException when socket fails
     * @throws InterruptedException when sleep interrupted
     */
    @Test
    void testFirstPutReturns201Then200() throws IOException, InterruptedException {
        File tempData = createTempStationFile("lllllllll", 20, 55);

        ContentServer contentServer = new ContentServer();
        contentServer.startConnection("localhost", TEST_PORT);

        HttpHelper.Response firstResponse = contentServer.sendData(tempData);
        assertTrue(firstResponse.status.contains("201"), "First PUT should return 201");

        HttpHelper.Response secondResponse = contentServer.sendData(tempData);
        assertTrue(secondResponse.status.contains("200"), "Subsequent PUT should return 200");

        contentServer.stopConnection();
    }

    /**
     * Test that a bad json that is missing the station id tag will return a 500 error
     *
     * @throws IOException when socket fails
     * @throws InterruptedException when sleep is interrupted
     */
    @Test
    void testInvalidJsonReturns500() throws IOException, InterruptedException {
        File invalidFile = File.createTempFile("invalid", ".txt");
        try (PrintWriter writer = new PrintWriter(invalidFile)) {
            writer.println("INVALID_JSON");
        }

        ContentServer contentServer = new ContentServer();
        contentServer.startConnection("localhost", TEST_PORT);

        HttpHelper.Response response = contentServer.sendData(invalidFile);
        assertTrue(response.status.contains("400"));

        contentServer.stopConnection();
    }

    /**
     * Test that no data is added or read if a non-matching station id is sent
     *
     * @throws IOException when socket fails
     * @throws InterruptedException when sleep is interrupted
     */
    @Test
    void testEmptyFileReturnsFileNotFound() throws IOException, InterruptedException {
        File emptyFile = File.createTempFile("empty", ".txt");

        ContentServer contentServer = new ContentServer();
        contentServer.startConnection("localhost", TEST_PORT);

        Exception exception = assertThrows(FileNotFoundException.class, () -> contentServer.sendData(emptyFile));
        assertTrue(exception.getMessage().contains("empty"));

        contentServer.stopConnection();
    }

    /**
     * Test multiple clients accessing data at the same time
     *
     * @throws IOException when socket fails
     * @throws InterruptedException when sleep is interrupted
     * @throws ExecutionException when a thread fails to execute
     */
    @Test
    void testMultipleClientsGetDataSimultaneously() throws IOException, InterruptedException, ExecutionException {
        File tempData = createTempStationFile("MULTI123", 18, 50);

        ContentServer contentServer = new ContentServer();
        contentServer.startConnection("localhost", TEST_PORT);
        contentServer.sendData(tempData);
        contentServer.stopConnection();

        ExecutorService clientPool = Executors.newFixedThreadPool(5);
        Callable<Boolean> getTask = () -> {
            GetClient client = new GetClient();
            client.startConnection("localhost", TEST_PORT);
            HttpHelper.Response resp = client.requestStationData("MULTI123");
            client.stopConnection();
            Map<String, String> data = SimpleJsonUtil.parse(resp.body);
            Map<String, String> stationData = SimpleJsonUtil.parse(data.get("MULTI123"));
            return "18".equals(stationData.get("air_temp")) && "50".equals(stationData.get("rel_hum"));
        };

        Future<Boolean>[] futures = new Future[5];
        for (int i = 0; i < 5; i++) {
            futures[i] = clientPool.submit(getTask);
        }

        for (Future<Boolean> f : futures) {
            assertTrue(f.get(), "All clients should retrieve the same correct data");
        }

        clientPool.shutdown();
    }

    /**
     * Test that stale data will be removed after 30 seconds
     *
     * @throws IOException when socket fails
     * @throws InterruptedException when sleep is interrupted
     */
    @Test
    void testStaleDataRemovalAfterTimeout() throws IOException, InterruptedException {
        File tempData = createTempStationFile("STALE123", 10, 40);

        ContentServer contentServer = new ContentServer();
        contentServer.startConnection("localhost", TEST_PORT);
        contentServer.sendData(tempData);
        contentServer.stopConnection();

        // Wait >30 seconds to allow stale data removal
        Thread.sleep(35_000);

        GetClient getClient = new GetClient();
        getClient.startConnection("localhost", TEST_PORT);
        HttpHelper.Response getResponse = getClient.requestStationData("STALE123");
        getClient.stopConnection();

        assertFalse(getResponse.status.contains("200") && getResponse.body.contains("STALE123"),
                "Data should have been removed as stale");
    }

    /**
     * Helper function = creates a temporary station file
     * @param id stationId
     * @param airTemp recognisable data
     * @param relHum recognisable data
     * @return A Temp Station File
     * @throws IOException if writing fails
     */
    private File createTempStationFile(String id, int airTemp, int relHum) throws IOException {
        File tempData = File.createTempFile("station", ".txt");
        try (PrintWriter writer = new PrintWriter(tempData)) {
            writer.println("id:" + id);
            writer.println("name:Test Station");
            writer.println("state:TS");
            writer.println("time_zone:CST");
            writer.println("lat:0.0");
            writer.println("lon:0.0");
            writer.println("local_date_time:01/01:00am");
            writer.println("local_date_time_full:20250101000000");
            writer.println("air_temp:" + airTemp);
            writer.println("apparent_t:" + airTemp);
            writer.println("cloud:Clear");
            writer.println("dewpt:0.0");
            writer.println("press:1013.0");
            writer.println("rel_hum:" + relHum);
            writer.println("wind_dir:N");
            writer.println("wind_spd_kmh:0");
            writer.println("wind_spd_kt:0");
        }
        return tempData;
    }
}
