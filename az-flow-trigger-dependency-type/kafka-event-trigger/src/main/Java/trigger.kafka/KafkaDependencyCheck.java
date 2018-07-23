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

import azkaban.flowtrigger.DependencyCheck;
import azkaban.flowtrigger.DependencyInstanceCallback;
import azkaban.flowtrigger.DependencyInstanceConfig;
import azkaban.flowtrigger.DependencyInstanceContext;
import azkaban.flowtrigger.DependencyInstanceRuntimeProps;
import azkaban.flowtrigger.DependencyPluginConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.LoggerFactory;
import trigger.kafka.Constants.DependencyInstanceConfigKey;
import trigger.kafka.Constants.DependencyPluginConfigKey;


/**
 * A KafkaDependencyCheck is a factory in our design integrate with configuration file.
 * So the class does a lot of validation on users define parameters.
 * Also, keep tract with all dependencies.
 *
 */

public class KafkaDependencyCheck implements DependencyCheck {
  private final static org.slf4j.Logger log = LoggerFactory.getLogger(KafkaDependencyCheck.class);
  private final ExecutorService executorService;
  private KafkaEventMonitor dependencyMonitor;

  public KafkaDependencyCheck() {
    this.executorService = Executors.newSingleThreadExecutor();
  }

  public boolean remove(final DependencyInstanceContext depContext) {
    final KafkaDependencyInstanceContext depContextCasted = (KafkaDependencyInstanceContext) depContext;
    this.dependencyMonitor.remove(depContextCasted);
    return true;
  }

  private void validate(final DependencyInstanceConfig config, final DependencyInstanceRuntimeProps runtimeProps) {
    final String LOG_SUFFIX =
        String.format("for dependency name: %s",config.get(DependencyInstanceConfigKey.NAME));

    final String topic = config.get(DependencyInstanceConfigKey.TOPIC);
    final String match = config.get(DependencyInstanceConfigKey.MATCH);
    Preconditions.checkNotNull(topic, DependencyInstanceConfigKey.TOPIC + " cannot be null " + LOG_SUFFIX);
    Preconditions.checkNotNull(match, DependencyInstanceConfigKey.MATCH + " cannot be null " + LOG_SUFFIX);
  }

  @Override
  public DependencyInstanceContext run(final DependencyInstanceConfig config,
      final DependencyInstanceRuntimeProps runtimeProps, final DependencyInstanceCallback callback) {
    this.validate(config, runtimeProps);
    final KafkaDependencyInstanceContext depInstance =
        new KafkaDependencyInstanceContext(config, this, callback);

    this.dependencyMonitor.add(depInstance);
    return depInstance;
  }

  @Override
  public void shutdown() {
    log.info("Shutting down KafkaDependencyCheck");
    // disallow new tasks
    this.executorService.shutdown();

    try {
      // interrupt current threads;
      this.executorService.shutdownNow();
      // Wait a while for tasks to respond to being cancelled
      if (!this.executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        log.error("KafkaDependencyCheck does not terminate.");
      }
    } catch (final InterruptedException ex) {
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void init(final DependencyPluginConfig config) {
    //need to add an required for dependency check
    final Set<String> required =
        Sets.newHashSet(DependencyPluginConfigKey.KAKFA_BROKER_URL);
    for (final String requiredField : required) {
      Preconditions.checkNotNull(config.get(requiredField), requiredField + " is required");
    }
    this.dependencyMonitor = new KafkaEventMonitor(config);
    try {
      Future<?> result = this.executorService.submit(this.dependencyMonitor);
    } catch (RuntimeException rte) {
      log.debug("[cichang] ExecutorService for minitor with RuntimeException: ", rte);
    }

  }
}
