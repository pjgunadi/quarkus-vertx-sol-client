package pg.exp.lab;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "solace")
public interface Solace {
    String ip();
    String port();
    boolean useTls();
    String vpn();
    String username();
    String password();
    boolean sslValidate();
    int retries();
    int waitMs();
    String topic();
    String queueName();
    boolean clientAck();
    boolean persistent();
    int pubDelayMs();
    String payload();
}
