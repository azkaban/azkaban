package trigger.kafka;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaDepInstanceCollection {

  private final static Logger log = LoggerFactory.getLogger(KafkaDepInstanceCollection.class);
  public final Map<String, Map<String, List<KafkaDependencyInstanceContext>>> topicEventMap;

  public KafkaDepInstanceCollection() {
    this.topicEventMap = new HashMap<>();
  }

  public synchronized void add(final KafkaDependencyInstanceContext dep) {
    final String topic = dep.getTopicName();
    Map<String, List<KafkaDependencyInstanceContext>> eventMap = this.topicEventMap.get(topic);
    List<KafkaDependencyInstanceContext> depList;
    if (eventMap == null) {
      eventMap = new HashMap<>();
      depList = new LinkedList<>();
    } else {
      depList = eventMap.get(dep.getRegexMatch());
      if (depList == null) {
        depList = new LinkedList<>();
      }
    }
    depList.add(dep);
    eventMap.put(dep.getRegexMatch(), depList);
    this.topicEventMap.put(topic, eventMap);
  }

  //print function for debugging
  public void streamTopicToEvent(final Map<String, Map<String, List<KafkaDependencyInstanceContext>>> map) {
    map.entrySet().stream().forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));
  }

  public void streamEventToDep(final Map<String, List<KafkaDependencyInstanceContext>> map) {
    map.entrySet().stream().forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));
  }

  public boolean hasTopic(final String topic) {
    return !(this.topicEventMap.get(topic) == null);
  }

  public List<String> getTopicList() {
    final List<String> res = new ArrayList<>(this.topicEventMap.keySet());
    return res;
  }

  public Set<String> hasEventInTopic(final String topic, final RegexKafkaDependencyMatcher matcher,
      final String payload) {
    final Set<String> res = new HashSet<>();
    final Map<String, List<KafkaDependencyInstanceContext>> eventMap = this.topicEventMap.get(topic);
    if (eventMap == null) {
      return null;
    }
    for (final Map.Entry<String, List<KafkaDependencyInstanceContext>> entry : eventMap.entrySet()) {
      if (matcher.isMatch(payload, entry.getKey())) {
        res.add(entry.getKey());
      }
    }
    return res;
  }

  public synchronized List<KafkaDependencyInstanceContext> getDepsByTopicAndEvent(final String topic,
      final String event) {
    final Map<String, List<KafkaDependencyInstanceContext>> eventMap = this.topicEventMap.get(topic);
    if (eventMap != null) {
      return eventMap.get(event);
    }
    return null;
  }

  public synchronized void remove(final KafkaDependencyInstanceContext dep) {
    final Map<String, List<KafkaDependencyInstanceContext>> eventMap = this.topicEventMap.get(dep.getTopicName());
    if (eventMap != null) {
      final List<KafkaDependencyInstanceContext> deps = eventMap.get(dep.getRegexMatch());
      if (deps != null) {
        final Iterator<KafkaDependencyInstanceContext> it = deps.iterator();
        while (it.hasNext()) {
          final KafkaDependencyInstanceContext curr = it.next();
          if (curr == dep) {
            it.remove();
            break;
          }
        }
        if (deps.isEmpty()) {
          eventMap.remove(dep.getRegexMatch());
        }
        if (eventMap.isEmpty()) {
          this.topicEventMap.remove(dep.getTopicName());
        }
      }
    }
  }

  public synchronized boolean removeList(final String topic, final String event,
      final List<KafkaDependencyInstanceContext> list) {
    final List<String> ori = new ArrayList<>(this.topicEventMap.keySet());
    final Map<String, List<KafkaDependencyInstanceContext>> eventMap = this.topicEventMap.get(topic);
    if (eventMap != null) {
      final List<KafkaDependencyInstanceContext> deps = eventMap.get(event);
      if (deps != null) {
        deps.removeAll(list);
      }
      if (deps.isEmpty()) {
        eventMap.remove(event);
      }
      if (eventMap.isEmpty()) {
        this.topicEventMap.remove(topic);
      }
    }
    final List<String> res = new ArrayList<>(this.topicEventMap.keySet());
    return res == ori;
  }

  @Override
  public String toString() {
    final Joiner.MapJoiner mapJoiner = Joiner.on("\n").withKeyValueSeparator("=");
    return mapJoiner.join(this.topicEventMap);
  }
}
