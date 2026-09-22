package co.wethinkcode.logisticsconnect;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public class TransitServiceApp {

    private static final String HUB_URL   = "http://localhost:7051/hubs/";
    private static final String DELAY_URL = "http://localhost:7052/delay-stage/";


    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient CLIENT = HttpClient.newHttpClient();

    public static final long BASE_ETA_MINUTES = 120;
    public static final long  MINUTES_PER_STAGE = 30;

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7053);

        app.get("/health", ctx -> ctx.result("OK"));

        // TODO (Calculates estimated arrival windows based on hub and delay stage.)
        // Add domain endpoints for transit-service here.

        app.get("/eta/{hubId}", ctx -> {
            String hubId = ctx.pathParam("hubId").toUpperCase();
            try {
                Hub hub = getJson(HUB_URL + hubId, Hub.class);
                DelayStage ds = getJson(DELAY_URL + hubId, DelayStage.class);

                long minutes    = BASE_ETA_MINUTES + ds.stage() * MINUTES_PER_STAGE;
                Instant arrival = Instant.now().plus(Duration.ofMinutes(minutes));

                ctx.json(new EtaResponse(
                        hub.hubId(),
                        hub.province(),
                        hub.sortingCenter(),
                        ds.stage(),
                        arrival.toString(),
                        minutes));
            } catch (UpstreamNotFound e) {
                ctx.status(404).json(Map.of("error", e.getMessage()));
            } catch (Exception e) {
                ctx.status(502).json(Map.of("error", "upstream failure: " + e.getMessage()));
            }

        });
    }
    private static <T> T getJson(String url, Class<T> type) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url)).GET().build();
        HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString());

        if (resp.statusCode() == 404) {
            throw new UpstreamNotFound("hub not found at upstream: " + url);
        }
        if (resp.statusCode() != 200) {
            throw new RuntimeException("upstream " + resp.statusCode() + " from " + url);
        }
        return MAPPER.readValue(resp.body(), type);
    }

    public record Hub(String hubId, String province, String sortingCenter) {}
    public record DelayStage(String hubId, int stage) {}
    public record EtaResponse(String hubId, String province, String sortingCenter,
                              int delayStage, String estimatedArrival, long etaMinutes) {}

    static class UpstreamNotFound extends Exception {
        UpstreamNotFound(String m) {
            super(m);
        }
    }
}

// MQ TODO: subscribes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.logisticsconnect.mq.MqConfig)
