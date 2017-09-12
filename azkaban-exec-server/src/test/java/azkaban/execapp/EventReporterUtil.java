package azkaban.execapp;

import static org.mockito.Mockito.mock;

import azkaban.execapp.reporter.AzkabanKafkaAvroEventReporter;
import azkaban.spi.AzkabanEventReporter;
import azkaban.utils.Props;
import gobblin.metrics.kafka.KafkaAvroEventReporter;
import org.mockito.Mockito;

public final class EventReporterUtil {
  private EventReporterUtil() {

  }

  /**
   *
   * @return - Returns a mock <code>AzkabanEventReporter</code> instance.
   */
  public static AzkabanEventReporter getTestAzkabanEventReporter() {
    final KafkaAvroEventReporter kafkaAvroEventReporter = mock(KafkaAvroEventReporter.class);
    Mockito.doNothing().when(kafkaAvroEventReporter).report();
    return new AzkabanKafkaAvroEventReporter(kafkaAvroEventReporter, getTestKafkaProps());
  }

  /**
   * @return - Returns required kafka properties for test methods.
   */
  public static Props getTestKafkaProps() {
    final Props kafkaProps = new Props();
    kafkaProps.put("azkaban.event.reporting.kafka.brokers", "brokers.com:10950");
    kafkaProps.put("azkaban.event.reporting.kafka.topic", "KafkaTopic");
    kafkaProps
        .put("azkaban.event.reporting.kafka.schema.registry.url", "registry.com/schemaRegistry");
    return kafkaProps;
  }
}
