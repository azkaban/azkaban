package azkaban.utils;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_KAFKA_PROXY_USER;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_LOGGING_KAFKA_CLASS_PARAM;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_LOGGING_KAFKA_ENABLED;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.executor.KeyStoreManager;
import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Objects;
import org.apache.kafka.log4jappender.KafkaLog4jAppender;
import org.apache.log4j.Layout;
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
      final Layout layout, final String execId, final String name, final String jobAttempt,
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
      final String kafkaKeyStoreName =
          props.getString(AZKABAN_KAFKA_PROXY_USER, "azkafka");

      KafkaLog4jAppender kafkaLog4jAppender;
      try {
        // Pass in KeyStore for Kafka authentication if possible.
        final Constructor<?> kafkaLog4jAppenderClassConstructorWithKeyStore =
            kafkaLog4jAppenderClass.getConstructor(KeyStore.class);
        KeyStore keyStore = KeyStoreManager.getInstance().getKeyStoreMap().get(kafkaKeyStoreName);
        Preconditions.checkNotNull(keyStore);
        kafkaLog4jAppender =
            (KafkaLog4jAppender) kafkaLog4jAppenderClassConstructorWithKeyStore.newInstance(keyStore);
      } catch (NoSuchMethodException | NullPointerException ex) {
        final Constructor<?> kafkaLog4jAppenderClassConstructor =
            kafkaLog4jAppenderClass.getConstructor();
        kafkaLog4jAppender = (KafkaLog4jAppender) kafkaLog4jAppenderClassConstructor.newInstance();
      }

      String[] origIds = new String[] {execId, name, jobAttempt};
      String[] nonNullIds = Arrays.stream(origIds).filter(Objects::nonNull).toArray(String[]::new);

      kafkaLog4jAppender.setBrokerList(props
          .getString(Constants.ConfigurationKeys.AZKABAN_LOGGING_KAFKA_BROKERS));
      kafkaLog4jAppender.setTopic(props.getString(topicConfigKey));

      // Concat necessary information for the custom appender to consume.
      kafkaLog4jAppender.setName(getAzkabanKafkaLog4jName(nonNullIds));
      kafkaLog4jAppender.setLayout(layout);
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
   * @param layout Log4j layout
   * @param execId Azkaban exec id
   * @param name Azkaban flow id
   * @return KafkaLog4jAppender
   */
  public static KafkaLog4jAppender getAzkabanFlowKafkaLog4jAppender(final Props props,
      final Layout layout, final String execId, final String name) {
    return getAzkabanKafkaLog4jAppender(props, layout, execId, name, null,
        ConfigurationKeys.AZKABAN_FLOW_LOGGING_KAFKA_TOPIC);
  }

  /**
   * Get Azkaban Kafka Log4j Appender for given Azkaban job.
   *
   * @param props Azkaban props
   * @param layout Log4j layout
   * @param execId Azkaban exec id
   * @param name Azkaban job's nested id
   * @param jobAttempt Azkaban job attempt
   * @return KafkaLog4jAppender
   */
  public static KafkaLog4jAppender getAzkabanJobKafkaLog4jAppender(final Props props,
      final Layout layout, final String execId, final String name, final String jobAttempt) {
    return getAzkabanKafkaLog4jAppender(props, layout, execId, name, jobAttempt,
        ConfigurationKeys.AZKABAN_JOB_LOGGING_KAFKA_TOPIC);
  }
}
