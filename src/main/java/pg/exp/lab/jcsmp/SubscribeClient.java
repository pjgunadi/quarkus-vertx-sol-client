package pg.exp.lab.jcsmp;

import com.solacesystems.jcsmp.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class SubscribeClient extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(SubscribeClient.class);
    private JCSMPSession session;

    @Override
    public void start() throws JCSMPException {
        logger.info("Starting Topic Subscriber");
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

        //Create Solace Connection Properties
        String solace_host = String.format("%s:%s", ip, port);
        if (useTls) {
            solace_host = String.format("tcps://%s", solace_host);
        }

        //Create Properties
        final JCSMPProperties props = new JCSMPProperties();
        props.setProperty(JCSMPProperties.HOST, solace_host);
        props.setProperty(JCSMPProperties.VPN_NAME, vpn_name);
        props.setProperty(JCSMPProperties.USERNAME, username);
        props.setProperty(JCSMPProperties.PASSWORD, password);
        props.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, ssl_val);
        props.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES_RECONNECT_RETRIES, retries);
        props.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES_RECONNECT_RETRY_WAIT_IN_MILLIS, waitms);

        //Connect to Solace
        session = JCSMPFactory.onlyInstance().createSession(props);
        session.connect();

        //Start Listener
        vertx.executeBlocking(h -> this.receiveMessage(h, topicName))
          .onSuccess(h -> logger.info("Listener result: {}", h.toString()));

    }

    private void receiveMessage(Promise<Object> promise, String topicName) {
        final Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);
        try {
            //Subscribe Topic
            session.addSubscription(topic);
            //Assign Consumer object
            XMLMessageConsumer consumer = session.getMessageConsumer(
              new JCSMPReconnectEventHandler() {
                  @Override
                  public boolean preReconnect() {
                      logger.info("Reconnecting...");
                      return true;
                  }

                  @Override
                  public void postReconnect() throws JCSMPException {
                      session.addSubscription(topic);
                      logger.info("Subscribed Topic: {}", topic);
                  }
              }, new XMLMessageListener() {
                  @Override
                  public void onReceive(BytesXMLMessage msg) {
                      logger.info("Received a message");
                      logger.debug("Message Dump:\n{}", msg.dump());


                      //Processing message
                      CompletableFuture.supplyAsync(() -> processMessage(msg))
                        .thenApplyAsync(x -> {

                            logger.debug("Thinking of doing something...");

                            return new CompletableFuture<>().complete("Completed");
                        });
                      logger.debug("End of message processing.");
                  }

                  @Override
                  public void onException(JCSMPException e) {
                      logger.error("Consumer has exception: {}}", e.getMessage());
                  }
              });
            //Start Consumer
            consumer.start();
            logger.info("Subscribed Topic: {}", topicName);
            promise.complete("Completed");
        } catch (JCSMPException e) {
            e.printStackTrace();
        }
    }

    private CompletableFuture<String> processMessage(BytesXMLMessage msg) {
        logger.debug("Processing message...");
        CompletableFuture<String> futureResult = new CompletableFuture<>();
        String bodyMsg = new String(msg.getAttachmentByteBuffer().array(), StandardCharsets.UTF_8);
        logger.info("Message: {}", bodyMsg);

        futureResult.complete("Completed");

        return futureResult;
    }

}
