package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import co.wethinkcode.logisticsconnect.mq.MqConfig;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class AlertBotApp {

    /**Stage 5 >= Alert will be posted
     * Stage 5 < Alert will not be posted
     * **/

    private static final int ALERT_THRESHOLD = 5;


    public static void main(String[] args) throws Exception {
        startSubscriber();

        Javalin app = Javalin.create().start(7054);
        app.get("/health", ctx -> ctx.result("OK"));
    }

        // TODO (Posts proactive delay notifications to public transit social media pages (simulated).)
        // Mechanism: Outbound webhook, simulated social post
        private static void startSubscriber() throws JMSException {
            ConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
            Connection connection = factory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create and Subscribe to the Topic
            Topic topic = session.createTopic(MqConfig.TOPIC);
            MessageConsumer consumer = session.createConsumer(topic);

            consumer.setMessageListener(msg -> {
                try {
                    if (msg instanceof TextMessage tm) {
                        String json = tm.getText();
                        String hubId = extractString(json, "hubId");
                        int stage = extractInt(json, "stage");
                        if (hubId == null) return;

                        System.out.println("[MQ] received -> " + json);

                        if (stage >= ALERT_THRESHOLD) {
                            simulateSocialPost(hubId, stage);
                        } else {
                            System.out.println("[alert] " + hubId + " stage " + stage
                                    + " below threshold " + ALERT_THRESHOLD + " no alert");

                        }
                    }

                    } catch (JMSException e) {
                        System.err.println("[MQ] receive failed: " + e.getMessage());
                    }
                });

                System.out.println("[MQ] subscriber listening on topic '" + MqConfig.TOPIC
                    + "' (alert threshold = " + ALERT_THRESHOLD + ")");

            }
            private static void simulateSocialPost(String hubId, int stage) {
                System.out.println("📣 SIMULATED SOCIAL POST");
                System.out.println("   To:      @LogisticsConnect (public feed)");
                System.out.println("   Subject: Severe delays at " + hubId);
                System.out.println("   Body:    \u26a0\ufe0f Delay stage " + stage + "/8 at " + hubId
                        + ". Expect extended transit times. We will update when the situation improves.");
                System.out.println("   Status:  POSTED \u2713");
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
                while (end < json.length()
                        && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
                    end++;
                }
                return Integer.parseInt(json.substring(start, end));


    }
}

// MQ TODO (stretch goal): subscribes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.logisticsconnect.mq.MqConfig)
