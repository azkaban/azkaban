package azkaban.spi;

import java.util.Map;

/**
 * Implement this interface to report flow and job events. Event reporter
 * can be turned on by setting the property {@code AZKABAN_EVENT_REPORTING_ENABLED} to true.
 *
 * By default, a KafkaAvroEventReporter is provided. Alternate implementations
 * can be provided by setting the property {@code AZKABAN_EVENT_REPORTING_CLASS_PARAM}
 * <br><br>
 * The constructor will be called with a {@code azkaban.utils.Props} object passed as
 * the only parameter. If such a constructor doesn't exist, then the AzkabanEventReporter
 * instantiation will fail.
 */
public interface AzkabanEventReporter {

  boolean report(EventType eventType, Map<String, String> metadata);
}
