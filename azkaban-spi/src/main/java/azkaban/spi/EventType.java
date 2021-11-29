package azkaban.spi;

/**
 * Enum class defining the list of supported event types.
 */
public enum EventType {
  // Executor event type
  FLOW_STARTED,
  FLOW_FINISHED,
  JOB_STARTED,
  JOB_FINISHED,
  JOB_STATUS_CHANGED,
  EXTERNAL_FLOW_UPDATED,
  EXTERNAL_JOB_UPDATED,
  FLOW_STATUS_CHANGED,
  //Project event type
  USER_PERMISSION_CHANGED,
  GROUP_PERMISSION_CHANGED,
  PROJECT_UPLOADED,
  SCHEDULE_CREATED,
  JOB_PROPERTY_OVERRIDDEN,
  // User login/logout event types:
  USER_LOGIN,
  USER_LOGOUT
}
