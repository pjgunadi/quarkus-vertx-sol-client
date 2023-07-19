package pg.exp.lab;

import com.google.gson.Gson;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    @ConfigProperty(name="client.publisher")
    Integer numPublishers;

    @ConfigProperty(name="client.subscriber.direct")
    Integer numSubscribersDirect;

    @ConfigProperty(name="client.subscriber.guaranteed")
    Integer numSubscribersGuaranteed;

    @Inject
    Solace solaceConfig;

    public void init(@Observes StartupEvent e, Vertx x) {
        logger.info("Deploying Verticle");
        logger.info("# Publishers: {},\n# Direct Subscribers: {}\n# Guaranteed Subscribers: {}", numPublishers.toString(), numSubscribersDirect.toString(), numSubscribersGuaranteed.toString());

        String strJson = new Gson().toJson(solaceConfig);
        logger.debug("solaceConfig:\n{}", strJson);
        JsonObject config = new JsonObject(strJson);
        logger.debug("config:\n{}", config.encodePrettily());
        DeploymentOptions dpOptions = new DeploymentOptions();
        dpOptions.setConfig(config);
        logger.debug("dpOptions:\n{}", dpOptions.toJson().encodePrettily());

        //Deploy Verticles

        if (numPublishers.intValue()>0)
            x.deployVerticle("pg.exp.lab.jcsmp.PublishClient", dpOptions.setInstances(numPublishers.intValue()));
        if (numSubscribersDirect.intValue()>0)
            x.deployVerticle("pg.exp.lab.jcsmp.SubscribeClient", dpOptions.setInstances(numSubscribersDirect.intValue()));
        if (numSubscribersGuaranteed.intValue()>0)
            x.deployVerticle("pg.exp.lab.jcsmp.EndpointClient", dpOptions.setInstances(numSubscribersGuaranteed.intValue()));
    }
}
