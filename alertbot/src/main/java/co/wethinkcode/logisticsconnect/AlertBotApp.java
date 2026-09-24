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




            });



    }
}

// MQ TODO (stretch goal): subscribes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.logisticsconnect.mq.MqConfig)
