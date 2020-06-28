package azkaban.spi;

/**
 * Enum class defining the list of supported event types.
 */
public enum EventType {
  FLOW_STARTED,
  FLOW_FINISHED,
  JOB_STARTED,
  JOB_FINISHED,
  JOB_STATUS_CHANGED,
  EXTERNAL_FLOW_UPDATED,
  EXTERNAL_JOB_UPDATED,
  // For Kafka event schema enrichment
  FLOW_STATUS_CHANGED
}
