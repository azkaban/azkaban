package trigger.kafka;
import azkaban.flowtrigger.DependencyPluginConfig;
import azkaban.flowtrigger.DependencyInstanceContext;
import azkaban.flowtrigger.DependencyInstanceConfig;
import azkaban.flowtrigger.DependencyPluginConfigImpl;
import azkaban.flowtrigger.DependencyInstanceConfigImpl;
import azkaban.flowtrigger.DependencyInstanceRuntimePropsImpl;
import azkaban.flowtrigger.DependencyInstanceCallback;
import azkaban.flowtrigger.DependencyInstanceRuntimeProps;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.HashMap;
import trigger.kafka.Constants.*;


public class Test {
  public static void main(final String[] args) {
    final KafkaDependencyCheck check = new KafkaDependencyCheck();
    final Map<String, String> pluginConfigMap = new HashMap<>();
    final String brokerURL = "localhost:9092";
    final String schemaRegistryRestfulClient = "http://ltx1-schemaregistry-vip-1.corp.linkedin.com:10252/schemaRegistry/schemas";


    pluginConfigMap.put(DependencyPluginConfigKey.KAKFA_BROKER_URL, brokerURL);
    pluginConfigMap.put(DependencyPluginConfigKey.SCHEMA_REGISTRY_URL, schemaRegistryRestfulClient);

    final DependencyPluginConfig pluginConfig = new DependencyPluginConfigImpl(pluginConfigMap);
    check.init(pluginConfig);
    createContext(check, "AzEvent_Topic", "chiawei_start","1");

  }

  private static DependencyInstanceContext createContext(final KafkaDependencyCheck check,
      final String topic, final String event,final String counter) {
    final Map<String, String> props = new HashMap<>();

    props
        .put(DependencyInstanceConfigKey.EVENT, event);
    props.put(DependencyInstanceConfigKey.TOPIC, topic);
    props.put(DependencyInstanceConfigKey.COUNTER, counter);

    final DependencyInstanceConfig instConfig = (DependencyInstanceConfig)new DependencyInstanceConfigImpl(props);
    final Map<String, String> runtimePropsMap = new HashMap<>();
    runtimePropsMap.put("startTime", String.valueOf(System.currentTimeMillis()));
    final DependencyInstanceRuntimeProps runtimeProps =(DependencyInstanceRuntimeProps) new DependencyInstanceRuntimePropsImpl(runtimePropsMap);

    final DependencyInstanceCallback callback = (DependencyInstanceCallback)new DependencyInstanceCallbackI();
    final DependencyInstanceContext context = check.run(instConfig, runtimeProps, callback);
    return context;
  }

}
