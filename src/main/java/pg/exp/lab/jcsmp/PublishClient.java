package pg.exp.lab.jcsmp;

import com.solacesystems.jcsmp.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class PublishClient extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(PublishClient.class);
    private JCSMPSession session;

    @Override
    public void start() throws JCSMPException {
        logger.info("Starting Publisher");
        JsonObject ctxCfg = vertx.getOrCreateContext().config();
        logger.debug("config:\n{}", ctxCfg.encodePrettily());

        //Set Variables
        final String ip = ctxCfg.getString("ip");
        final String port = ctxCfg.getString("port");
        final Boolean useTls = ctxCfg.getBoolean("useTls");
        final String vpn_name = ctxCfg.getString("vpn");
        final String username = ctxCfg.getString("username");
        final String password = ctxCfg.getString("password");
        final Boolean ssl_val = ctxCfg.getBoolean("sslValidate");
        final Integer retries = ctxCfg.getInteger("retries");
        final Integer waitms = ctxCfg.getInteger("waitMs");
        final String topicName = ctxCfg.getString("topic");
        final Boolean persistent = ctxCfg.getBoolean("persistent");
        final Integer pubDelayMs = ctxCfg.getInteger("pubDelayMs");
        final String payload = ctxCfg.getString("payload");

        //Create Solace Connection Properties
        String solace_host = String.format("%s:%s", ip, port);
        if(useTls) {
            solace_host = String.format("tcps://%s",solace_host);
        }

        //Create Properties
        final JCSMPProperties props = new JCSMPProperties();
        props.setProperty(JCSMPProperties.HOST, solace_host);
        props.setProperty(JCSMPProperties.VPN_NAME,vpn_name);
        props.setProperty(JCSMPProperties.USERNAME, username);
        props.setProperty(JCSMPProperties.PASSWORD, password);
        props.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, ssl_val);
        props.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES_RECONNECT_RETRIES, retries);
        props.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES_RECONNECT_RETRY_WAIT_IN_MILLIS, waitms);

        //Connect to Solace
        session = JCSMPFactory.onlyInstance().createSession(props);
        session.connect();

        //Create Message Object
        BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);

        //Create Topic
        final Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);

        //Create Producer
        XMLMessageProducer prod = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override
            public void responseReceivedEx(Object o) {
                if(!Objects.isNull(o)) {
                    logger.info("Producer received response from msg: {}", o.toString());
                }
            }

            @Override
            public void handleErrorEx(Object o, JCSMPException e, long l) {
                if(!Objects.isNull(o)) {
                    logger.warn("Producer received error for msg: {}@{} - {}", o.toString(), l, e.getMessage());
                } else {
                    logger.warn("Producer received error for msg: {} - {}", l, e.getMessage());
                }
            }

        });


        //Publish Periodically
        vertx.setPeriodic(pubDelayMs,id -> {
            logger.info("Publishing Message...");
            msg.clearContent();
            if (persistent) {
                msg.setDeliveryMode(DeliveryMode.PERSISTENT);
            } else {
                msg.setDeliveryMode(DeliveryMode.DIRECT);
            }
            String txtMsg = new String("{message: \""+ payload + "\"}");
            logger.info("Publishing to topic: {}. Payload: {}", topicName, txtMsg);
            msg.setData(txtMsg.getBytes(StandardCharsets.UTF_8));
            try {
                prod.send(msg, topic);
                logger.debug("Sent");
            } catch (JCSMPException e) {
                throw new RuntimeException(e);
            }
        });
    }

}