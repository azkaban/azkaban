package trigger.kafka;

import avro.shaded.com.google.common.collect.ImmutableMap;
import azkaban.flowtrigger.DependencyInstanceConfig;
import azkaban.flowtrigger.DependencyInstanceConfigImpl;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;
import static trigger.kafka.Constants.*;


public class KafkaDepInstanceCollectionTest {

  static KafkaDependencyInstanceContext createContext(final String topic, final String match, final long startTime,
      final String depName) {

    final Map<String, String> props =
        ImmutableMap.of(DependencyInstanceConfigKey.TOPIC, topic, DependencyInstanceConfigKey.MATCH, match,
            DependencyInstanceConfigKey.NAME, depName);

    final DependencyInstanceConfig config = new DependencyInstanceConfigImpl(props);

    final KafkaDependencyCheck depCheck = new KafkaDependencyCheck();
    final KafkaDependencyInstanceContext res =
        new KafkaDependencyInstanceContext(config, depCheck, null);
    return res;
  }

  private List<KafkaDependencyInstanceContext> createContextList(final String dateString) throws ParseException {
    final List<KafkaDependencyInstanceContext> contexts = new ArrayList<>();

    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    sdf.setTimeZone(TimeZone.getTimeZone("PST"));
    final Date date = sdf.parse(dateString);

    KafkaDependencyInstanceContext context =
        this.createContext("AzTest_Topic1", "^(\\\\d{3}-?\\\\d{2}-?\\\\d{4})$", date.getTime(), "dep1");
    contexts.add(context);
    context = this.createContext("AzTest_Topic1", "hadoop.*", date.getTime(), "dep2");
    contexts.add(context);
    context = this.createContext("AzTest_Topic2", "^\\w*", date.getTime(), "dep3");
    contexts.add(context);
    context = this.createContext("AzTest_Topic3", ".*", date.getTime(), "dep4");
    contexts.add(context);

    return contexts;
  }

  private void createContextListAndAddToCollection(final String dateString, final KafkaDepInstanceCollection collection)
      throws ParseException {
    final List<KafkaDependencyInstanceContext> contextList = this.createContextList(dateString);
    for (final KafkaDependencyInstanceContext context : contextList) {
      collection.add(context);
    }
  }

  @Test
  public void testAddAndGet() throws ParseException {
    final KafkaDepInstanceCollection testMap = new KafkaDepInstanceCollection();
    this.createContextListAndAddToCollection("2018-06-01 01:00:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 01:20:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 01:30:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 01:40:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 01:50:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 02:00:00", testMap);

    assertThat(testMap.toString()).isEqualToIgnoringWhitespace(
        "AzTest_Topic3={.*=[trigger.kafka.KafkaDependencyInstanceContext@2cb4c3ab, trigger.kafka.KafkaDependencyInstanceContext@13c78c0b, trigger.kafka.KafkaDependencyInstanceContext@12843fce, trigger.kafka.KafkaDependencyInstanceContext@3dd3bcd, trigger.kafka.KafkaDependencyInstanceContext@97e1986, trigger.kafka.KafkaDependencyInstanceContext@26f67b76]}"
            + "\nAzTest_Topic2={^\\w*=[trigger.kafka.KafkaDependencyInstanceContext@153f5a29, trigger.kafka.KafkaDependencyInstanceContext@7f560810, trigger.kafka.KafkaDependencyInstanceContext@69d9c55, trigger.kafka.KafkaDependencyInstanceContext@13a57a3b, trigger.kafka.KafkaDependencyInstanceContext@7ca48474, trigger.kafka.KafkaDependencyInstanceContext@337d0578]}"
            + "\nAzTest_Topic1={^(\\\\d{3}-?\\\\d{2}-?\\\\d{4})$=[trigger.kafka.KafkaDependencyInstanceContext@59e84876, trigger.kafka.KafkaDependencyInstanceContext@61a485d2, trigger.kafka.KafkaDependencyInstanceContext@39fb3ab6, trigger.kafka.KafkaDependencyInstanceContext@6276ae34, trigger.kafka.KafkaDependencyInstanceContext@7946e1f4, trigger.kafka.KafkaDependencyInstanceContext@3c09711b], hadoop.*=[trigger.kafka.KafkaDependencyInstanceContext@5cc7c2a6, trigger.kafka.KafkaDependencyInstanceContext@b97c004, trigger.kafka.KafkaDependencyInstanceContext@4590c9c3, trigger.kafka.KafkaDependencyInstanceContext@32e6e9c3, trigger.kafka.KafkaDependencyInstanceContext@5056dfcb, trigger.kafka.KafkaDependencyInstanceContext@6574b225]}");

    assertThat(testMap.getDepsByTopicAndEvent("a", "b")).isNull();
    assertThat(testMap.hasTopic("AzTest_Topic3")).isTrue();
    assertThat(testMap.getDepsByTopicAndEvent("AzTest_Topic1", "hadoop.*").toString()).isEqualTo(
        "[trigger.kafka.KafkaDependencyInstanceContext@5cc7c2a6, trigger.kafka.KafkaDependencyInstanceContext@b97c004, trigger.kafka.KafkaDependencyInstanceContext@4590c9c3, trigger.kafka.KafkaDependencyInstanceContext@32e6e9c3, trigger.kafka.KafkaDependencyInstanceContext@5056dfcb, trigger.kafka.KafkaDependencyInstanceContext@6574b225]");
  }

  @Test
  public void testRemove() throws ParseException {
    final KafkaDepInstanceCollection testMap = new KafkaDepInstanceCollection();
    this.createContextListAndAddToCollection("2018-06-01 01:00:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 01:20:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 01:30:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 01:40:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 01:50:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 02:00:00", testMap);
    final List<KafkaDependencyInstanceContext> contexts = testMap.getDepsByTopicAndEvent("AzTest_Topic1", "hadoop.*");
    for (final KafkaDependencyInstanceContext context : new ArrayList<>(contexts)) {
      testMap.remove(context);
    }
    assertThat(testMap.getDepsByTopicAndEvent("AzTest_Topic1", "hadoop.*")).isNull();
    assertThat(testMap.hasTopic("AzTest_Topic1")).isTrue();
  }

  @Test
  public void testRemoveList() throws ParseException {
    final KafkaDepInstanceCollection testMap = new KafkaDepInstanceCollection();
    this.createContextListAndAddToCollection("2018-06-01 01:00:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 01:20:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 01:30:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 01:40:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 01:50:00", testMap);
    this.createContextListAndAddToCollection("2018-06-01 02:00:00", testMap);
    testMap.removeList("AzTest_Topic3", ".*", testMap.getDepsByTopicAndEvent("AzTest_Topic3", ".*"));
    assertThat(testMap.getDepsByTopicAndEvent("AzTest_Topic3", ".*")).isNull();
    assertThat(testMap.hasTopic("AzTest_Topic3")).isFalse();
  }
}
