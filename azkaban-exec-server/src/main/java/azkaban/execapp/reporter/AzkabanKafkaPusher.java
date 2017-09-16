package azkaban.execapp.reporter;

import com.google.common.io.Closer;
import gobblin.metrics.kafka.KafkaPusher;
import gobblin.metrics.kafka.ProducerCloseable;
import java.util.Properties;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;


/**
 * Extends the KafkaPusher class to create an async type kafka producer.
 */
public class AzkabanKafkaPusher extends KafkaPusher {

  private static final Logger logger = Logger.getLogger(AzkabanKafkaPusher.class.getName());

  AzkabanKafkaPusher(final String brokers, final String topic) {
    super(brokers, topic);
  }

  @Override
  public ProducerCloseable<String, byte[]> createProducer(final ProducerConfig config) {
    final Properties props = config.props().props();
    props.put("producer.type", "async");
    final ProducerConfig newConfig = new ProducerConfig(props);
    logger.info("Kafka producer type is set to " + newConfig.producerType());
    return Closer.create().register(new ProducerCloseable<String, byte[]>(newConfig));
  }
}
