package azkaban.utils;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_LOGGING_KAFKA_CLASS_PARAM;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_LOGGING_KAFKA_ENABLED;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Objects;
import org.apache.kafka.log4jappender.KafkaLog4jAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for getting Azkaban Kafka Log4j Appender
 */
public class KafkaLog4jUtils {

  private static final Logger logger = LoggerFactory.getLogger(KafkaLog4jUtils.class);

  private static String getAzkabanKafkaLog4jName(final String[] ids) {
    return String.join(".", ids);
  }

  private static KafkaLog4jAppender getAzkabanKafkaLog4jAppender(final Props props,
      final String execId, final String flowId, final String jobId, final String jobAttempt,
      final String topicConfigKey) {
    final boolean loggingKafkaEnabled =
        props.getBoolean(AZKABAN_LOGGING_KAFKA_ENABLED, false);

    if (!loggingKafkaEnabled) {
      logger.info("Server logging kafka is not enabled");
      return null;
    }

    try {
      final Class<?> kafkaLog4jAppenderClass =
          props.getClass(AZKABAN_LOGGING_KAFKA_CLASS_PARAM, KafkaLog4jAppender.class);
      logger.info("Loading kafka log4j appender class " + kafkaLog4jAppenderClass.getName());

      final Constructor<?> kafkaLog4jAppenderClassConstructor =
          kafkaLog4jAppenderClass.getConstructor();
      KafkaLog4jAppender kafkaLog4jAppender =
          (KafkaLog4jAppender) kafkaLog4jAppenderClassConstructor.newInstance();

      String[] origIds = new String[] {execId, flowId, jobId, jobAttempt};
      String[] nonNullIds = Arrays.stream(origIds).filter(Objects::nonNull).toArray(String[]::new);

      kafkaLog4jAppender.setBrokerList(props
          .getString(Constants.ConfigurationKeys.AZKABAN_LOGGING_KAFKA_BROKERS));
      kafkaLog4jAppender.setTopic(props.getString(topicConfigKey));

      // Concat necessary information for the custom appender to consume.
      kafkaLog4jAppender.setName(getAzkabanKafkaLog4jName(nonNullIds));
      kafkaLog4jAppender.activateOptions();

      return kafkaLog4jAppender;
    } catch (final Exception e) {
      logger.error("Could not instantiate KafkaLog4jAppender", e);
      return null;
    }
  }

  /**
   * Get Azkaban Kafka Log4j Appender for given Azkaban flow.
   *
   * @param props Azkaban props
   * @param execId Azkaban exec id
   * @param flowId Azkaban flow id
   * @return KafkaLog4jAppender
   */
  public static KafkaLog4jAppender getAzkabanFlowKafkaLog4jAppender(final Props props,
      final String execId, final String flowId) {
    return getAzkabanKafkaLog4jAppender(props, execId, flowId, null, null,
        ConfigurationKeys.AZKABAN_FLOW_LOGGING_KAFKA_TOPIC);
  }

  /**
   * Get Azkaban Kafka Log4j Appender for given Azkaban job.
   *
   * @param props Azkaban props
   * @param execId Azkaban exec id
   * @param flowId Azkaban flow id
   * @param jobId Azkaban job id
   * @param jobAttempt Azkaban job attempt
   * @return KafkaLog4jAppender
   */
  public static KafkaLog4jAppender getAzkabanJobKafkaLog4jAppender(final Props props,
      final String execId, final String flowId, final String jobId, final String jobAttempt) {
    return getAzkabanKafkaLog4jAppender(props, execId, flowId, jobId, jobAttempt,
        ConfigurationKeys.AZKABAN_JOB_LOGGING_KAFKA_TOPIC);
  }
}
