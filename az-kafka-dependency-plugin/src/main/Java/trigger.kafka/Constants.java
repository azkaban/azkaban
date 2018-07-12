package trigger.kafka;

public class Constants {
  public static class DependencyPluginConfigKey {

    public static final String KAKFA_BROKER_URL = "kafka.broker.url";
    public static final String SCHEMA_REGISTRY_URL = "kafka.schema.registry.url";
    public static final String KEY_STORE_FILE = "keystore.file";
    public static final String KEY_STORE_PASSWORD = "keystore.password";
    public static final String TRUST_STORE_FILE = "truststore.file";
    public static final String TRUST_STORE_PASSWORD = "truststore.password";
  }

  public static class DependencyInstanceConfigKey {
    public static final String MATCH = "match";
    public static final String TOPIC = "topic";
    public static final String NAME = "name";
  }

  public static class DependencyInstanceRuntimeConfigKey {
    public static final String START_TIME = "startTime";
    public static final String TRIGGER_INSTANCE_ID = "triggerInstanceId";
  }
}
