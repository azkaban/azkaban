package trigger.kafka;

import azkaban.flowtrigger.DependencyPluginConfig;
import com.linkedin.kafka.clients.consumer.LiKafkaConsumerImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trigger.kafka.Constants.DependencyPluginConfigKey;
public class KafkaEventMonitor implements Runnable {
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);
    private final KafkaDepInstanceCollection depInstances;
    private final static Logger log = LoggerFactory.getLogger(KafkaEventMonitor.class);
    private Consumer<String, byte[]>consumer;
    private LiKafkaConsumerImpl consumer1;
    //private Consumer<String, String> consumer;
    private final ConcurrentLinkedQueue<String> subscribedTopics = new ConcurrentLinkedQueue<>();
    private Schema schema;

    public KafkaEventMonitor(final DependencyPluginConfig pluginConfig) {
        initKafkaClient(pluginConfig);
        this.consumer.subscribe(Arrays.asList("AzEvent_Init_Topic"));
        if (!this.subscribedTopics.isEmpty()) {
            ConsumerSubscriptionRebalance();
        }
        //initialize deserialize schema
        String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
          "\"name\": \"User\"," + "\"fields\": [{\"name\": \"name\", \"type\": \"string\"},"
           + "{\"name\": \"username\", \"type\": \"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        this.schema = parser.parse(userSchema);
        this.depInstances = new KafkaDepInstanceCollection();
    }
    private void initKafkaClient(final DependencyPluginConfig pluginConfig) {
        Properties props = new Properties();
        props.put("bootstrap.servers", pluginConfig.get(DependencyPluginConfigKey.KAKFA_BROKER_URL));
        props.put("auto.commit.interval.ms", "1000");
        props.put("enable.auto.commit", "true");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer",ByteArrayDeserializer.class);
        //props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id","test-consumer-group");
       // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<String, byte[]>(props);
//        Properties props1 = new Properties();
//        props1.put("bootstrap.servers", pluginConfig.get(DependencyPluginConfigKey.KAKFA_BROKER_URL));
//        props1.put("auto.commit.interval.ms", "1000");
//        props1.put("enable.auto.commit", "true");
//        props1.put("group.id","test-consumer-group");
//        props1.put("key.deserializer", StringDeserializer.class.getName());
//        props1.put("value.deserializer",ByteArrayDeserializer.class);
//        props1.put("value.deserializer",StringDeserializer.class);
//        props1.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, pluginConfig.get(DependencyPluginConfigKey.SCHEMA_REGISTRY_URL));
//        props1.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
//        this.consumer1 = new LiKafkaConsumerImpl(props1);
//        VerifiableProperties vProps = new VerifiableProperties(props);
//        KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
//        KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);
//        this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
//        Map<String, Integer> topicCountMap = new HashMap<>();
//        Map<String, List<KafkaStream<Object, Object>>> consumerMap = this.consumer2.createMessageStreams(
//          topicCountMap, keyDecoder, valueDecoder);

    }
    public void add(final KafkaDependencyInstanceContext context) {
        this.executorService.submit(() -> {
            try {
                System.out.printf("ready to add %s\n", context.getDepName());
                if (!this.depInstances.hasTopic(context.getTopicName())) {
                    this.depInstances.add(context);
                    subscribedTopics.addAll(this.depInstances.getTopicList());
                }
                else
                    this.depInstances.add(context);
            }catch (final Exception ex) {
                //log.error(String.format("unable to add context %s, cancelling it", context), ex);
                System.out.printf("Cancle Here : %s", ex);
                context.getCallback().onCancel(context);
            }
        });
    }
    public void remove(final KafkaDependencyInstanceContext context) {
        this.depInstances.remove(context);
        if(!this.depInstances.hasTopic(context.getTopicName())){
            subscribedTopics.addAll(this.depInstances.getTopicList());
        }
    }
    @Override
    public void run() {
        System.out.println("==============In RUN===========");
        try {
            while (true && !Thread.interrupted()) {
                System.out.println(consumer.subscription());
                System.out.println("Above ConsumerRecords");
                ConsumerRecords<String, byte[]> records = consumer.poll(10000);
                    //ConsumerRecords<String, String> records = consumer.poll(10000);
                Record recordToProcess = null;
                System.out.println("Below ConsumerRecords");
                for (ConsumerRecord<String, byte[]> record : records){
                    System.out.println("---------------"+record+"-----------------");
                    try{
                        System.out.printf("1.Kafka get %s from TOPIC: %s\n",record.topic(),record.value());
                        DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(this.schema);
                        Decoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
                        GenericRecord payload2 = reader.read(null, decoder);
                        System.out.println("Message received : " + payload2);
                        String eventName = payload2.get("name").toString();
//
//                        System.out.printf("Kafka get %s from TOPIC: %s\n",record.topic(),eventName);
                        System.out.printf("2.Kafka get %s from TOPIC: %s\n",record.topic(),eventName);

    //                    SpecificDatumReader<Object> reader = new SpecificDatumReader<>(this.schema);
    //                    ByteArrayInputStream is = new ByteArrayInputStream(record.value());
    //                    BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(is, null);
    //                    String log = datumReader.read(null, binaryDecoder);
    //                    System.out.println("Value: " + log);

                        if (this.depInstances.hasEventInTopic(record.topic(),eventName)) {
                            System.out.println("hasEventinTopic\n");
                            List<KafkaDependencyInstanceContext> deleteList = new LinkedList<>();
                            final List<KafkaDependencyInstanceContext> possibleAvailableDeps =
                                this.depInstances.getDepsByTopicAndEvent(record.topic(),eventName);
                            for (final KafkaDependencyInstanceContext dep : possibleAvailableDeps) {
                                if (dep.eventCaptured() == 0) {
                                    log.info(String.format("dependency %s becomes available, sending success " + "callback",
                                        dep));
                                    dep.getCallback().onSuccess(dep);
                                    deleteList.add(dep);
                                }
                            }
                            System.out.println("back from success");
                            if (!this.depInstances.removeList(record.topic(), eventName, deleteList))
                                subscribedTopics.addAll(this.depInstances.getTopicList());
                        }
                    }catch (final Exception ex) {
                        // todo: find a better way to handle schema evolution, just fail silently and let the
                        // last check handle this.
                        // currently we just swallow the exception
                        log.error("failure when parsing record " + recordToProcess, ex);
                        System.out.printf("%s",ex);
                    }
                }
                System.out.println("==============------===========");
                depInstances.streamTopicToEvent(depInstances.topicEventMap);
                if (!this.subscribedTopics.isEmpty()) {
                    ConsumerSubscriptionRebalance();
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
    public synchronized void ConsumerSubscriptionRebalance(){
        System.out.println("updating......");
        if (!subscribedTopics.isEmpty()) {
            Iterator<String> iter = subscribedTopics.iterator();
            List<String> topics = new ArrayList<>();
            while (iter.hasNext()) {
                topics.add(iter.next());
            }
            subscribedTopics.clear();
            this.consumer.subscribe(topics); //re-subscribe
        }
    }

}
