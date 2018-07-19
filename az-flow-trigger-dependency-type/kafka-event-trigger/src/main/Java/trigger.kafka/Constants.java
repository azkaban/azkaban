package trigger.kafka;

public class Constants {
  public static class DependencyPluginConfigKey {

    public static final String KAKFA_BROKER_URL = "kafka.broker.url";
    public static final String SCHEMA_REGISTRY_URL = "kafka.schema.registry.url";
  }

  public static class DependencyInstanceConfigKey {
    public static final String NAME = "name";
    public static final String TOPIC = "topic";
    public static final String MATCH = "match";
  }

  public static class DependencyInstanceRuntimeConfigKey {
    public static final String START_TIME = "startTime";
    public static final String TRIGGER_INSTANCE_ID = "triggerInstanceId";
  }
}
