package azkaban.utils;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_FLOW_LOGGING_KAFKA_TOPIC;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_JOB_LOGGING_KAFKA_TOPIC;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_LOGGING_KAFKA_BROKERS;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_LOGGING_KAFKA_CLASS_PARAM;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_LOGGING_KAFKA_ENABLED;

import org.apache.kafka.log4jappender.KafkaLog4jAppender;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KafkaLog4jUtilTest {

  private final Props props = new Props();
  private final String expectedKafkaBrokers = "sample.kafka.azkaban.com:12345";
  private final String expectedFlowLoggingKafkaTopic = "sample_flow_logging_kafka_topic";
  private final String expectedJobLoggingKafkaTopic = "sample_flow_logging_kafka_topic";
  private final String execId = "12345";
  private final String flowId = "sample_flowid";
  private final String jobId = "sample_jobid";
  private final String jobAttempt = "1";

  @Before
  public void setUp() {
    props.put(AZKABAN_LOGGING_KAFKA_ENABLED, "true");
    props.put(AZKABAN_LOGGING_KAFKA_CLASS_PARAM, MockKafkaLog4jAppender.class.getName());
    props.put(AZKABAN_LOGGING_KAFKA_BROKERS, expectedKafkaBrokers);
    props.put(AZKABAN_FLOW_LOGGING_KAFKA_TOPIC, expectedFlowLoggingKafkaTopic);
    props.put(AZKABAN_JOB_LOGGING_KAFKA_TOPIC, expectedJobLoggingKafkaTopic);
  }

  @Test
  public void returnNullIfLoggingKafkaNotEnabled() {
    KafkaLog4jAppender appender;
    props.put(AZKABAN_LOGGING_KAFKA_ENABLED, "false");
    appender = KafkaLog4jUtils.getAzkabanFlowKafkaLog4jAppender(props, execId,
        flowId);
    Assert.assertNull(appender);
    appender = KafkaLog4jUtils.getAzkabanJobKafkaLog4jAppender(props, execId, flowId, jobId, jobAttempt);
    Assert.assertNull(appender);
  }

  @Test
  public void returnNullIfKafkaClassIsNotFound() {
    props.put(AZKABAN_LOGGING_KAFKA_CLASS_PARAM, "unknown_class");
    KafkaLog4jAppender appender;
    appender = KafkaLog4jUtils.getAzkabanFlowKafkaLog4jAppender(props, execId,
        flowId);
    Assert.assertNull(appender);
    appender = KafkaLog4jUtils.getAzkabanJobKafkaLog4jAppender(props, execId, flowId, jobId, jobAttempt);
    Assert.assertNull(appender);
  }

  @Test
  public void validateFields() {
    final String expectedFlowAppenderName = execId + "." + flowId;
    final String expectedJobAppenderName = execId + "." + flowId + "." + jobId + "." + jobAttempt;

    KafkaLog4jAppender appender;
    appender = KafkaLog4jUtils.getAzkabanFlowKafkaLog4jAppender(props, execId, flowId);
    Assert.assertEquals(appender.getBrokerList(), expectedKafkaBrokers);
    Assert.assertEquals(appender.getTopic(), expectedFlowLoggingKafkaTopic);
    Assert.assertEquals(appender.getName(), expectedFlowAppenderName);

    appender = KafkaLog4jUtils.getAzkabanJobKafkaLog4jAppender(props, execId, flowId, jobId, jobAttempt);
    Assert.assertEquals(appender.getBrokerList(), expectedKafkaBrokers);
    Assert.assertEquals(appender.getTopic(), expectedJobLoggingKafkaTopic);
    Assert.assertEquals(appender.getName(), expectedJobAppenderName);
  }

  public static class MockKafkaLog4jAppender extends KafkaLog4jAppender {
    @Override
    public void activateOptions() {}
  }
}
