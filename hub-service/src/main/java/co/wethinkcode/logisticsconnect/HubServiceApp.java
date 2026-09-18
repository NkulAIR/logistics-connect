package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HubServiceApp {

    private static final String INGESTION_URL = "http://localhost:7050/hubs";
    private static final ObjectMapper MAPPER  = new ObjectMapper();
    private static final HttpClient   CLIENT  = HttpClient.newHttpClient();

    public record Hub(String hubId, String province, String sortingCenter, Boolean active) {}
    private static final Map<String, Hub> HUBS = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        try {
            loadHubsFromIngestion();
        } catch (Exception e) {
            throw new RuntimeException("Could not load hubs", e);
        }

        Javalin app = Javalin.create().start(7051);
        app.get("/health", ctx -> ctx.result("OK"));

        // TODO (Serves provinces and sorting centers (place-name source of truth).)
        // Add domain endpoints for hub-service here.
        app.get("/hubs", ctx -> ctx.json(HUBS.values()));

        app.get("/hubs/{hubId}", ctx ->{
            String id = ctx.pathParam("hubId").toUpperCase();
            Hub hub = HUBS.get(id);
            if (hub == null) ctx.status(404).json(Map.of("error", "hub not found: " + id));
            else ctx.json(hub);
        });

        app.get("/provinces", ctx -> ctx.json(
                HUBS.values().stream()
                        .map(Hub::province).filter(p -> p != null)
                        .distinct().sorted().toList()));

        app.get("/sorting-centers", ctx -> ctx.json(
                HUBS.values().stream()
                        .map(Hub::sortingCenter).filter(s -> s != null)
                        .distinct().sorted().toList()));

    }
    /**
     * CONNECTS THE SERVICE TO INGESTIONHUB via HTTP Request
     **/

    private static void loadHubsFromIngestion() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(INGESTION_URL)).GET().build();
        HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString());

        if (resp.statusCode() != 200) {
            throw new RuntimeException("ingestion-service returned HTTP " + resp.statusCode());
        }
        List<Hub> hubs = MAPPER.readValue(resp.body(), new TypeReference<>() {});
        for (Hub h : hubs) HUBS.put(h.hubId(), h);
        System.out.println("Loaded " + HUBS.size() + " hubs from ingestion-service");
    }



}
