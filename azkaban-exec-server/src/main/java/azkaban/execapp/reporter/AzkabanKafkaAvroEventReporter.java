package azkaban.execapp.reporter;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_EVENT_REPORTING_KAFKA_BROKERS;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_EVENT_REPORTING_KAFKA_SCHEMA_REGISTRY_URL;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_EVENT_REPORTING_KAFKA_TOPIC;

import azkaban.spi.AzkabanEventReporter;
import azkaban.spi.EventType;
import azkaban.utils.Props;
import com.google.common.base.Preconditions;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.kafka.KafkaAvroEventReporter;
import gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import gobblin.metrics.kafka.KafkaEventReporter;
import gobblin.metrics.kafka.KafkaPusher;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;


/**
 * Default implementation of the <code>AzkabanEventReporter</code> class.
 * An instance of this class is injected via guice into the <code>FlowRunnerManager</code>
 * class when event reporting is enabled.
 */
public class AzkabanKafkaAvroEventReporter implements AzkabanEventReporter {

  private static final Logger logger = Logger.getLogger(AzkabanKafkaAvroEventReporter.class);
  private static final String ROOT_CONTEXT_NAME = "AzkabanKafkaEvents";
  private static final String AZKABAN_FLOW_EVENTS_NAMESPACE = "AzkabanFlowEvents";
  private static final String AZKABAN_JOB_EVENTS_NAMESPACE = "AzkabanJobEvents";
  private final Properties kafkaProps = new Properties();
  private MetricContext rootContext;
  private KafkaAvroEventReporter kafkaAvroEventReporter;

  // For testing only.
  public AzkabanKafkaAvroEventReporter(final KafkaAvroEventReporter kafkaAvroEventReporter,
      final Props props) {
    this.kafkaAvroEventReporter = kafkaAvroEventReporter;
    initKafkaProperties(props);
  }

  // Constructed via guice provider
  public AzkabanKafkaAvroEventReporter(final Props props) {

    initKafkaProperties(props);

    final String eventReportingKafkaBrokers =
        props.get(AZKABAN_EVENT_REPORTING_KAFKA_BROKERS);
    final String eventReportingKafkaTopic =
        props.get(AZKABAN_EVENT_REPORTING_KAFKA_TOPIC);

    this.rootContext = MetricContext.builder(ROOT_CONTEXT_NAME).build();
    final KafkaEventReporter.Builder<? extends KafkaAvroEventReporter.Builder> builder =
        KafkaAvroEventReporter.Factory.forContext(this.rootContext);
    final KafkaPusher kafkaPusher =
        new AzkabanKafkaPusher(eventReportingKafkaBrokers, eventReportingKafkaTopic);

    try {
      this.kafkaAvroEventReporter = builder.withKafkaPusher(kafkaPusher)
          .withSchemaRegistry(new KafkaAvroSchemaRegistry(this.kafkaProps))
          .build(eventReportingKafkaBrokers, eventReportingKafkaTopic);
      logger.info("Initialized a new kafkaAvroEventReporter");
    } catch (final Exception e) {
      logger.error("Exception while initializing kafka reporter", e);
    }
  }

  private void initKafkaProperties(final Props props) {
    Preconditions.checkArgument(props.containsKey(AZKABAN_EVENT_REPORTING_KAFKA_BROKERS),
        String.format("Property %s not provided.", AZKABAN_EVENT_REPORTING_KAFKA_BROKERS));
    Preconditions.checkArgument(props.containsKey(AZKABAN_EVENT_REPORTING_KAFKA_TOPIC),
        String.format("Property %s not provided.", AZKABAN_EVENT_REPORTING_KAFKA_TOPIC));
    Preconditions
        .checkArgument(props.containsKey(AZKABAN_EVENT_REPORTING_KAFKA_SCHEMA_REGISTRY_URL),
            String.format("Property %s not provided.",
                AZKABAN_EVENT_REPORTING_KAFKA_SCHEMA_REGISTRY_URL));
    this.kafkaProps.put("kafka.schema.registry.url",
        props.get(AZKABAN_EVENT_REPORTING_KAFKA_SCHEMA_REGISTRY_URL));
  }

  /**
   * @param eventType - The event type to be reported via kafka.
   * @param metaData - Additional data to accompany the event.
   */
  @Override
  public boolean report(final EventType eventType, final Map<String, String> metaData) {
    if (this.kafkaAvroEventReporter != null) {
      switch (eventType) {
        case FLOW_STARTED:
          getEventSubmitter(AZKABAN_FLOW_EVENTS_NAMESPACE).submit("flowStarted", metaData);
          break;
        case FLOW_FINISHED:
          getEventSubmitter(AZKABAN_FLOW_EVENTS_NAMESPACE).submit("flowFinished", metaData);
          break;
        case JOB_STARTED:
          getEventSubmitter(AZKABAN_JOB_EVENTS_NAMESPACE).submit("jobStarted", metaData);
          break;
        case JOB_FINISHED:
          getEventSubmitter(AZKABAN_JOB_EVENTS_NAMESPACE).submit("jobFinished", metaData);
          break;
        default:
          logger.warn("Failed to report the event. Unrecognized event type "
              + eventType.name());
          return false;
      }
      logger.info("Sent " + eventType.name() + " event via kafka");
      this.kafkaAvroEventReporter.report();
    } else {
      logger.warn("Kafka reporter isn't initialized. Failed to report the event");
      return false;
    }
    return true;
  }

  /**
   * @param namespace - The event namespace
   * @return - Returns an <code>EventSubmitter</code>
   */
  private EventSubmitter getEventSubmitter(final String namespace) {
    return new EventSubmitter.Builder(this.rootContext, namespace).build();
  }
}
