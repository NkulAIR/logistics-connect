package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DelayStageServiceApp {

    private static final Map<String, Integer> STAGES = new ConcurrentHashMap<>();
    private static final int MIN_STAGE = 0;
    private static final int MAX_STAGE = 8;

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7052);

        app.get("/health", ctx -> ctx.result("OK"));

        // TODO (Tracks the Transit Delay Stage (0-8, e.g. weather shutdowns).)
        // Add domain endpoints for delay-stage-service here.
        app.get("/delay-stage/{hubId}", ctx ->{
            String hubId = ctx.pathParam("hubId").toUpperCase();
            int stage = STAGES.getOrDefault(hubId, 0);
            ctx.json(new DelayStage(hubId, stage));
        });

        app.post("/delay-stage/{hubId}", ctx -> {
            String hubId = ctx.pathParam("hubId").toUpperCase();

            DelayStageRequest body;
            try {
                body = ctx.bodyAsClass(DelayStageRequest.class);
            } catch (Exception e) {
                ctx.status(400).json(Map.of("error", "invalid JSON body"));
                return;
            }

            if (body.stage() < MIN_STAGE || body.stage() > MAX_STAGE) {
                ctx.status(400).json(Map.of("error",
                        "stage must be between " + MIN_STAGE + " and " + MAX_STAGE));
                return;
            }

            int previous = STAGES.getOrDefault(hubId, 0);
            STAGES.put(hubId, body.stage());
            System.out.println("Stage updated: " + hubId + " -> " + body.stage()
                    + " (was " + previous + ")");

            // STAGE 3 HOOK: publish to package-status-topic here.
            //   MqPublisher.publish(new PackageStatusMessage(
            //       hubId, body.stage(), Instant.now().toString()));

            ctx.json(new DelayStage(hubId, body.stage()));
        });
    }
    public record DelayStage(String hubId, int stage) {}
    public record DelayStageRequest(int stage) {}
}

// MQ TODO: publishes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.logisticsconnect.mq.MqConfig)
