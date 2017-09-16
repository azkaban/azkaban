package azkaban.execapp.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class AzkabanKafkaPusherTest {

  /**
   * Test validates that an async kafka producer is created.
   */
  @Test
  public void testCreateASyncKafkaProducer() {
    final AzkabanKafkaPusher azkabanKafkaPusher =
        new AzkabanKafkaPusher("broker.com:10251", "kafka.topic");
    assertThat(azkabanKafkaPusher).isNotNull();
  }

}
