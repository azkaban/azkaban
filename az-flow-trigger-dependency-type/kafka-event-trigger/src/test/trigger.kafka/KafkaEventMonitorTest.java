package trigger.kafka;

import azkaban.flowtrigger.DependencyPluginConfig;
import azkaban.flowtrigger.DependencyPluginConfigImpl;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;


public class KafkaEventMonitorTest {
  @Test
  public void testConsumerSubscriptionRebalance() throws ParseException {
    final Map<String, String> pluginConfigMap = new HashMap<>();
    final String brokerURL = "localhost:9092";

    pluginConfigMap.put(Constants.DependencyPluginConfigKey.KAKFA_BROKER_URL, brokerURL);
    final DependencyPluginConfig pluginConfig = new DependencyPluginConfigImpl(pluginConfigMap);
    KafkaEventMonitor testMonitor = new KafkaEventMonitor(pluginConfig);
    assertThat(testMonitor.getMonitorSubscription()).contains("AzEvent_Init_Topic");
    Date date = new Date();
    KafkaDependencyInstanceContext context =
        KafkaDepInstanceCollectionTest.createContext("AzTest_Topic1", "^(\\\\d{3}-?\\\\d{2}-?\\\\d{4})$",date.getTime(), "dep1");
    testMonitor.add(context);
    context = KafkaDepInstanceCollectionTest.createContext("AzTest_Topic1", "hadoop.*",date.getTime(), "dep2");
    testMonitor.add(context);
    context = KafkaDepInstanceCollectionTest.createContext("AzTest_Topic2", "^\\w*",date.getTime(), "dep3");
    testMonitor.add(context);
    context = KafkaDepInstanceCollectionTest.createContext("AzTest_Topic3", ".*",date.getTime(), "dep4");
    testMonitor.add(context);
    testMonitor.consumerSubscriptionRebalance();
    assertThat(testMonitor.getMonitorSubscription()).contains("AzTest_Topic1");
    assertThat(testMonitor.getMonitorSubscription()).contains("AzTest_Topic2");
    assertThat(testMonitor.getMonitorSubscription()).contains("AzTest_Topic3");
    assertThat(testMonitor.getMonitorSubscription()).doesNotContain("AzEvent_Init_Topic");
    testMonitor.remove(context);
    testMonitor.consumerSubscriptionRebalance();
    assertThat(testMonitor.getMonitorSubscription()).doesNotContain("AzTest_Topic3");
  }
}
