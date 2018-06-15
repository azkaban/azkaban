package trigger.kafka;

import azkaban.flowtrigger.DependencyPluginConfig;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import trigger.kafka.Constants.DependencyPluginConfigKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaEventMonitor implements Runnable {
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);
    private final KafkaDepInstanceCollection depInstances;
    private final static Logger log = LoggerFactory
        .getLogger(KafkaEventMonitor.class);
    private KafkaConsumer<String, String> consumer;

    public KafkaEventMonitor(final DependencyPluginConfig pluginConfig) {
        Properties props = new Properties();
        props.put("bootstrap.servers", pluginConfig.get(DependencyPluginConfigKey.KAKFA_BROKER_URL));
        props.put("auto.commit.interval.ms", "1000");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer",
            StringDeserializer.class.getName());
        props.put("value.deserializer",
            StringDeserializer.class.getName());
        props.put("group.id","test-consumer-group");

        this.depInstances = new KafkaDepInstanceCollection();

        this.consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("AzEvent_Topic"));
    }
    public void add(final KafkaDependencyInstanceContext context) {
        this.executorService.submit(() -> {
            try {
                System.out.printf("ready to add %s\n", context.getDepName());
                if (!this.depInstances.hasTopic(context.getTopicName())) {
                    consumer.subscribe(Arrays.asList(context.getTopicName()));
                }
                this.depInstances.add(context);
            }catch (final Exception ex) {
                log.error(String.format("unable to add context %s, cancelling it", context), ex);
                context.getCallback().onCancel(context);
            }
        });
    }
    public void remove(final KafkaDependencyInstanceContext context) {
        this.depInstances.remove(context);
        if (!this.depInstances.hasTopic(context.getTopicName())) {
            consumer.unsubscribe(context.getTopicName());
        }
    }
    @Override
    public void run() {
        System.out.println("==============In RUN===========");
        try {
            while (true && !Thread.interrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(10000);
                for (ConsumerRecord<String, String> record : records){
                    System.out.printf("Kafka get %s from TOPIC\n",record.value());
                    if (this.depInstances.hasEventInTopic(TOPIC,record.value())) {
                        System.out.println("hasEventinTopic\n");
                        List<KafkaDependencyInstanceContext> deleteList = new LinkedList<>();
                        final List<KafkaDependencyInstanceContext> possibleAvailableDeps =
                            this.depInstances.getDepsByTopicAndEvent(TOPIC, record.value().toString());
                        for (final KafkaDependencyInstanceContext dep : possibleAvailableDeps) {
                            if (dep.eventCaptured() == 0) {
                                log.info(String.format("dependency %s becomes available, sending success " + "callback",
                                    dep));
                                dep.getCallback().onSuccess(dep);
                                deleteList.add(dep);
                            }
                        }
                        System.out.println("back from success");
                        this.depInstances.removeList(TOPIC,record.value(),deleteList);
                    }
                    depInstances.streamTopicToEvent(depInstances.topicEventMap);
                }
            }
        } catch (final Exception ex) {
            log.error("failure when consuming kafka events", ex);
        } finally {
            //todo chren: currently there's an exception when closing:
            // Failed to send SSL Close message.
            this.consumer.close();
            log.info("kafka consumer closed...");
        }
    }

}
