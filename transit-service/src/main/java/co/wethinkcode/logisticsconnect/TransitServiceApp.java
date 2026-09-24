package co.wethinkcode.logisticsconnect;

import co.wethinkcode.logisticsconnect.mq.MqConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;


import javax.jms.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;
import com.fasterxml.jackson.databind.DeserializationFeature;

public class TransitServiceApp {

    private static final String HUB_URL   = "http://localhost:7051/hubs/";
    private static final String DELAY_URL = "http://localhost:7052/delay-stage/";


    private static final ObjectMapper MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final HttpClient CLIENT = HttpClient.newHttpClient();

    public static final long BASE_ETA_MINUTES = 120;
    public static final long  MINUTES_PER_STAGE = 30;

    private static final Map<String, Integer> STAGE_CACHE = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        new MqSubscriber().start();

        Javalin app = Javalin.create().start(7053);

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/eta/{hubId}", ctx -> {
            String hubId = ctx.pathParam("hubId").toUpperCase();
            try {
                Hub hub = getJson(HUB_URL + hubId, Hub.class);
                int stage = STAGE_CACHE.getOrDefault(hubId, 0);   // ← MQ-fed cache

                long minutes = BASE_ETA_MINUTES + stage * MINUTES_PER_STAGE;
                Instant arrival = Instant.now().plus(Duration.ofMinutes(minutes));

                ctx.json(new EtaResponse(
                        hub.hubId(),
                        hub.province(),
                        hub.sortingCenter(),
                        stage,
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

        if (resp.statusCode() == 404) throw new UpstreamNotFound("hub not found at upstream: " + url);
        if (resp.statusCode() != 200)
            throw new RuntimeException("upstream " + resp.statusCode() + " from " + url);
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
    /**
     * Plain-JMS subscriber. Runs in its own thread; on each message, updates STAGE_CACHE.
     */
    static class MqSubscriber {
        void start() throws JMSException {
            ConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
            Connection connection = factory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(MqConfig.TOPIC);
            MessageConsumer consumer = session.createConsumer(topic);

            consumer.setMessageListener(msg -> {
                try {
                    if (msg instanceof TextMessage tm) {
                        String json = tm.getText();
                        // Tiny hand-rolled parse — the payload shape is fixed by the producer.
                        String hubId = extractString(json, "hubId");
                        int stage    = extractInt(json, "stage");
                        if (hubId != null) {
                            STAGE_CACHE.put(hubId.toUpperCase(), stage);
                            System.out.println("[MQ] received -> " + json
                                    + " (cache: " + hubId.toUpperCase() + "=" + stage + ")");
                        }
                    }
                } catch (JMSException e) {
                    System.err.println("[MQ] receive failed: " + e.getMessage());
                }
            });

            System.out.println("[MQ] subscriber listening on topic '" + MqConfig.TOPIC + "'");
        }

        private static String extractString(String json, String key) {
            String needle = "\"" + key + "\":\"";
            int i = json.indexOf(needle);
            if (i < 0) return null;
            int start = i + needle.length();
            int end = json.indexOf('"', start);
            return end < 0 ? null : json.substring(start, end);
        }

        private static int extractInt(String json, String key) {
            String needle = "\"" + key + "\":";
            int i = json.indexOf(needle);
            if (i < 0) return 0;
            int start = i + needle.length();
            int end = start;
            while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
                end++;
            }
            return Integer.parseInt(json.substring(start, end));
        }
    }
}

// MQ TODO: subscribes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.logisticsconnect.mq.MqConfig)
