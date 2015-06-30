package azkaban.jobcallback;

public interface JobCallbackConstants {
  public static final String STATUS_TOKEN = "status";
  public static final String SEQUENCE_TOKEN = "sequence";
  public static final String HTTP_GET = "GET";
  public static final String HTTP_POST = "POST";

  public static final String MAX_POST_BODY_LENGTH_PROPERTY_KEY =
      "jobcallback.max.body.length";
  public static final int DEFAULT_POST_BODY_LENGTH = 4096;

  public static final String MAX_CALLBACK_COUNT_PROPERTY_KEY =
      "jobcallback.max_count";
  public static final int DEFAULT_MAX_CALLBACK_COUNT = 3;

  public static final String FIRST_JOB_CALLBACK_URL_TEMPLATE =
      "job.notification." + STATUS_TOKEN + ".1.url";

  public static final String JOB_CALLBACK_URL_TEMPLATE = "job.notification."
      + STATUS_TOKEN + "." + SEQUENCE_TOKEN + ".url";
  public static final String JOB_CALLBACK_REQUEST_METHOD_TEMPLATE =
      "job.notification." + STATUS_TOKEN + "." + SEQUENCE_TOKEN + ".method";

  public static final String JOB_CALLBACK_REQUEST_HEADERS_TEMPLATE =
      "job.notification." + STATUS_TOKEN + "." + SEQUENCE_TOKEN + ".headers";

  public static final String JOB_CALLBACK_BODY_TEMPLATE = "job.notification."
      + STATUS_TOKEN + "." + SEQUENCE_TOKEN + ".body";

  public static final String CONTEXT_SERVER_TOKEN = "?{server}";
  public static final String CONTEXT_PROJECT_TOKEN = "?{project}";
  public static final String CONTEXT_FLOW_TOKEN = "?{flow}";
  public static final String CONTEXT_EXECUTION_ID_TOKEN = "?{executionId}";
  public static final String CONTEXT_JOB_TOKEN = "?{job}";
  public static final String CONTEXT_JOB_STATUS_TOKEN = "?{status}";

  public static final String HEADER_ELEMENT_DELIMITER = "\r\n";
  public static final String HEADER_NAME_VALUE_DELIMITER = ":";
}
