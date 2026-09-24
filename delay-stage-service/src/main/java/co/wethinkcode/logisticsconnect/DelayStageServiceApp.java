package co.wethinkcode.logisticsconnect;

import co.wethinkcode.logisticsconnect.mq.MqConfig;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DelayStageServiceApp {

    private static final Map<String, Integer> STAGES = new ConcurrentHashMap<>();
    private static final int MIN_STAGE = 0;
    private static final int MAX_STAGE = 8;

    public static void main(String[] args) throws  Exception{
        MqPublisher publisher = new MqPublisher();
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

            // Build message
            if (body.stage() != previous) {
                String payload = String.format(
                        "{\"hubId\":\"%s\",\"stage\":%d,\"timestamp\":\"%s\"}",
                        hubId, body.stage(), Instant.now());
                publisher.publish(payload);
                System.out.println("Stage updated: " + hubId + " -> " + body.stage()
                        + " (was " + previous + ") [PUBLISHED]");
            } else {
                System.out.println("Stage unchanged: " + hubId + " = " + body.stage()
                        + " [no publish]");
            }
            ctx.json(new DelayStage(hubId, body.stage()));
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { publisher.close(); } catch (Exception ignored) {}
        }));
    }
    public record DelayStage(String hubId, int stage) {}
    public record DelayStageRequest(int stage) {}


    /**
     * Plain-JMS publisher to MqConfig.TOPIC at MqConfig.BROKER_URL.
     * One connection + session + producer held open for the life of the service.
     */
    static class MqPublisher implements  AutoCloseable{
        private final Connection connection;
        private final Session session;
        private final MessageProducer producer;

        MqPublisher() throws JMSException {
            ConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
            this.connection = factory.createConnection();
            this.connection.start();
            this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(MqConfig.TOPIC);
            this.producer = session.createProducer(topic);
            this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        }

        void publish(String json) {
            try {
                TextMessage msg = session.createTextMessage(json);
                producer.send(msg);
                System.out.println("[MQ] published -> " + json);
            } catch (JMSException e) {
                System.err.println("[MQ] publish failed: " + e.getMessage());
            }
        }
        @Override
        public void close() throws JMSException {
            producer.close();
            session.close();
            connection.close();
        }


    }

}

// MQ TODO: publishes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.logisticsconnect.mq.MqConfig)
