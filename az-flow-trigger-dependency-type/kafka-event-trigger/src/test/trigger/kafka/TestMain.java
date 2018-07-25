/*
 * Copyright 2018 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package trigger.kafka;

import azkaban.flowtrigger.DependencyInstanceCallback;
import azkaban.flowtrigger.DependencyInstanceConfig;
import azkaban.flowtrigger.DependencyInstanceConfigImpl;
import azkaban.flowtrigger.DependencyInstanceContext;
import azkaban.flowtrigger.DependencyInstanceRuntimeProps;
import azkaban.flowtrigger.DependencyInstanceRuntimePropsImpl;
import azkaban.flowtrigger.DependencyPluginConfig;
import azkaban.flowtrigger.DependencyPluginConfigImpl;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import trigger.kafka.Constants.DependencyInstanceConfigKey;
import trigger.kafka.Constants.DependencyPluginConfigKey;

/**
 * End to end testing from producing Kafka event to trigger a flow
 */
public class TestMain {
  public static void main(final String[] args) throws InterruptedException {
    final KafkaDependencyCheck check = new KafkaDependencyCheck();
    final Map<String, String> pluginConfigMap = new HashMap<>();
    final String brokerURL = "localhost:9092";
    final String schemaRegistryRestfulClient = "http://localhost:8000";

    pluginConfigMap.put(DependencyPluginConfigKey.KAKFA_BROKER_URL, brokerURL);
    final DependencyPluginConfig pluginConfig = new DependencyPluginConfigImpl(pluginConfigMap);
    check.init(pluginConfig);
    final DependencyInstanceContext Di1 = createContext(check, "AzEvent_Topic4", "cich.*");
    final DependencyInstanceContext Di2 = createContext(check, "AzEvent_Topic4", "^\\w*");
    final DependencyInstanceContext Di3 = createContext(check, "AzEvent_Topic4", "chiawei_start1");
    final DependencyInstanceContext Di4 = createContext(check, "AzEvent_Topic4", ".*");
    final DependencyInstanceContext Di5 = createContext(check, "AzEvent_Topic4", "stores.*store.*product");
    final DependencyInstanceContext Di6 = createContext(check, "AzEvent_Topic4", ".*");

    while (true) {
      final KafkaProducerTest Pt = new KafkaProducerTest("hadoop", "cichang");
      Thread.sleep(10000);
    }
  }

  private static DependencyInstanceContext createContext(final KafkaDependencyCheck check, final String topic,
      final String match) {
    final Map<String, String> props = new HashMap<>();

    props.put(DependencyInstanceConfigKey.TOPIC, topic);
    props.put(DependencyInstanceConfigKey.MATCH, match);

    final DependencyInstanceConfig instConfig = (DependencyInstanceConfig) new DependencyInstanceConfigImpl(props);
    final Map<String, String> runtimePropsMap = new HashMap<>();
    runtimePropsMap.put("startTime", String.valueOf(System.currentTimeMillis()));
    runtimePropsMap.put("triggerInstanceId", UUID.randomUUID().toString());
    final DependencyInstanceRuntimeProps runtimeProps =
        (DependencyInstanceRuntimeProps) new DependencyInstanceRuntimePropsImpl(runtimePropsMap);

    final DependencyInstanceCallback callback = (DependencyInstanceCallback) new DependencyInstanceCallbackI();
    final DependencyInstanceContext context = check.run(instConfig, runtimeProps, callback);
    return context;
  }
}
