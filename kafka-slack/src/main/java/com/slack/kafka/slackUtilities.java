package com.slack.kafka;

import com.slack.api.bolt.App;
import com.slack.api.bolt.AppConfig;
import com.slack.api.bolt.socket_mode.SocketModeApp;
import com.slack.api.model.event.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class slackUtilities {

    Logger logger = LoggerFactory.getLogger(slackUtilities.class.getName());
    String botToken = "xoxb-2111424141894-2118224818530-2RNlhA5AdXmkw8KJV6c99MFz";
    String appToken = "xapp-1-A023D3RTC5B-2118286974146-2697940300a7b8727766a8d2b32272020daa2fa0bd218741b56f958aedfefa8c";
    String bootstrapServer = "127.0.0.1:9092";

    public App createNewSlackApp() {
        return new App(AppConfig.builder().singleTeamBotToken(botToken).build());
    }

    public void initializeSlackProducer(App app, KafkaProducer<String, String> producer) {
        app.event(MessageEvent.class, (payload, ctx) -> {
            MessageEvent event = payload.getEvent();
            String msg = event.getText();
            logger.info(msg);
            producer.send(new ProducerRecord<>("slack_messages", null, msg), (recordMetadata, e) -> {
                if (e != null) logger.error("Error while sending\n", e);
            });
            return ctx.ack();
        });
    }

    public SocketModeApp getSocketModeApp(App app) throws Exception {
        return new SocketModeApp(appToken, app);
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return new KafkaProducer<>(properties);
    }

    public void run() throws Exception {
        App app = createNewSlackApp();
        KafkaProducer<String, String> producer = createKafkaProducer();
        initializeSlackProducer(app, producer);
        SocketModeApp socketModeApp = getSocketModeApp(app);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping App");
            try {
                socketModeApp.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
            producer.close();
        }));

        socketModeApp.start();
    }

    public static void main(String[] args) throws Exception {
        new slackUtilities().run();
    }
}
