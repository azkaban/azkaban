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

import azkaban.flowtrigger.DependencyPluginConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trigger.kafka.Constants.DependencyPluginConfigKey;

/**
 * A KafkaEventMonitor implements Azkaban trigger interface.
 * This class implements logic for kafka consumer and maintain the data structure for dependencies.
 *
 */
@SuppressWarnings("FutureReturnValueIgnored")
public class KafkaEventMonitor implements Runnable {
  private final static Logger log = LoggerFactory.getLogger(KafkaEventMonitor.class);
  private final ExecutorService executorService = Executors.newFixedThreadPool(4);
  private final KafkaDepInstanceCollection depInstances;
  private final ConcurrentLinkedQueue<String> subscribedTopics = new ConcurrentLinkedQueue<>();
  private Consumer<String,String> consumer;
  private static final String GROUP_ID = "group_" + KafkaEventMonitor.class.getSimpleName() + System.currentTimeMillis();

  public KafkaEventMonitor(final DependencyPluginConfig pluginConfig) {
    this.initKafkaClient(pluginConfig);
    this.consumer.subscribe(Arrays.asList("AzEvent_Init_Topic"));
    if (!this.subscribedTopics.isEmpty()) {
      this.consumerSubscriptionRebalance();
    }

    this.depInstances = new KafkaDepInstanceCollection();
  }

  private void initKafkaClient(final DependencyPluginConfig pluginConfig) {
    final Properties props = new Properties();
    props.put("bootstrap.servers", pluginConfig.get(DependencyPluginConfigKey.KAKFA_BROKER_URL));
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "latest");
    props.put("enable.auto.commit", "true");
    props.put("group.id", GROUP_ID);
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());

    this.consumer = new KafkaConsumer<String, String>(props);;
  }

  public void add(final KafkaDependencyInstanceContext context) {
    this.executorService.submit(() -> {
      try {
        if (!this.depInstances.hasTopic(context.getTopicName())) {
          this.depInstances.add(context);
          this.subscribedTopics.addAll(this.depInstances.getTopicList());
        } else {
          this.depInstances.add(context);
        }
      } catch (final Exception ex) {
        log.error(String.format("unable to add context %s, cancelling it", context), ex);
        context.getCallback().onCancel(context);
      }
    });
  }

  public void remove(final KafkaDependencyInstanceContext context) {
    this.depInstances.remove(context);
    if (!this.depInstances.hasTopic(context.getTopicName())) {
      this.subscribedTopics.addAll(this.depInstances.getTopicList());
    }
  }

  public Set<String> getMonitorSubscription(){
    return this.consumer.subscription();
  }

  @Override
  public void run() {
    try {
      while (true && !Thread.interrupted()) {
        if (!this.subscribedTopics.isEmpty()) {
          this.consumerSubscriptionRebalance();
        }
        final ConsumerRecords<String, String> records = this.consumer.poll(10000);
        final Record recordToProcess = null;
        for (final ConsumerRecord<String, String> record : records) {
          try {
            final String payload = record.value();
            final Set<String> matchedList = this.depInstances.eventsInTopic(record.topic(), payload);
            if (!matchedList.isEmpty()) {
              triggerDependencies(matchedList,record);
            }
          } catch (final Exception ex) {
            // todo: find a better way to handle schema evolution, just fail silently and let the
            // last check handle this.
            // currently we just swallow the exception
            log.error("failure when parsing record " + recordToProcess, ex);
          }
        }
        if (!this.subscribedTopics.isEmpty()) {
          this.consumerSubscriptionRebalance();
        }
      }
    } catch (final Exception ex) {
      log.error("failure when consuming kafka events", ex);
    } finally {
      // Failed to send SSL Close message.
      this.consumer.close();
      log.info("kafka consumer closed...");
    }
  }
  /**
   * Dynamically tuning subscription only for the topic that dependencies need.
   *
   */
  @VisibleForTesting
  synchronized void consumerSubscriptionRebalance() {
    log.debug("Subscribed Topics " + this.consumer.subscription());
    if (!this.subscribedTopics.isEmpty()) {
      final Iterator<String> iter = this.subscribedTopics.iterator();
      final List<String> topics = new ArrayList<>();
      while (iter.hasNext()) {
        topics.add(iter.next());
      }
      this.subscribedTopics.clear();
      //re-subscribe topics that are needed
      this.consumer.subscribe(topics);
    }
  }

  private void triggerDependencies(Set<String> matchedList,ConsumerRecord<String, String> record){
    final List<KafkaDependencyInstanceContext> deleteList = new LinkedList<>();
    for (final String it : matchedList) {
      final List<KafkaDependencyInstanceContext> possibleAvailableDeps =
          this.depInstances.getDepsByTopicAndEvent(record.topic(), it);
        for (final KafkaDependencyInstanceContext dep : possibleAvailableDeps) {
          dep.getCallback().onSuccess(dep);
          deleteList.add(dep);
        }
        //If dependencies that need to be removed could lead to unsubscribing topics, do the topics rebalance
        if (!this.depInstances.removeList(record.topic(), it, deleteList)) {
          this.subscribedTopics.addAll(this.depInstances.getTopicList());
        }
    }
  }
}
