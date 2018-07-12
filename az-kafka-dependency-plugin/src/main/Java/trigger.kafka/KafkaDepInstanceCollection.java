package trigger.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaDepInstanceCollection {

  private final static Logger log = LoggerFactory.getLogger(KafkaDepInstanceCollection.class);
  public final Map<String, Map<String,List<KafkaDependencyInstanceContext>>> topicEventMap;

  public KafkaDepInstanceCollection() {
    this.topicEventMap = new HashMap<>();
  }

  public synchronized void add(final KafkaDependencyInstanceContext dep) {
    final String topic = dep.getTopicName();
    Map<String,List<KafkaDependencyInstanceContext>> eventMap = this.topicEventMap.get(topic);
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
    this.topicEventMap.put(topic,eventMap);
  }
  public void streamTopicToEvent(Map<String, Map<String,List<KafkaDependencyInstanceContext>>> map){
    map.entrySet().stream().forEach(entry-> System.out.println(entry.getKey() +": "+entry.getValue()));
  }
  public void streamEventToDep(Map<String,List<KafkaDependencyInstanceContext>> map){
    map.entrySet().stream().forEach(entry-> System.out.println(entry.getKey() +": "+entry.getValue()));
  }

  public boolean hasTopic(String topic){
    return !(this.topicEventMap.get(topic)==null);
  }
  public List<String> getTopicList(){
    List<String> res = new ArrayList<String>(this.topicEventMap.keySet());
    return res;
  }
  //to do 小心。並且要測試這個ＮＵＬＬ的部分。因為ＪＡＶＡ不熟
  public  Set<String> hasEventInTopic(String topic, RegexKafkaDependencyMatcher matcher,GenericRecord payload){
    Set<String> res = new HashSet<>();
    Map<String,List<KafkaDependencyInstanceContext>> eventMap = this.topicEventMap.get(topic);
    if(eventMap==null){
      return null;
    }
    streamEventToDep(eventMap);//Null pointer
    for(Map.Entry<String, List<KafkaDependencyInstanceContext>> entry : eventMap.entrySet()){
        if(matcher.isMatch(payload,entry.getKey()))
          res.add(entry.getKey());
    }
    return res;
  }
  public synchronized List<KafkaDependencyInstanceContext> getDepsByTopicAndEvent(String topic, String event){
    final Map<String,List<KafkaDependencyInstanceContext>> eventMap = this.topicEventMap
        .get(topic);
    if(eventMap!=null){
      return eventMap.get(event);
    }
    return null;
  }

  public synchronized void remove(final KafkaDependencyInstanceContext dep) {
    Map<String,List<KafkaDependencyInstanceContext>> eventMap = this.topicEventMap
          .get(dep.getTopicName());
      if (eventMap != null) {
        final List<KafkaDependencyInstanceContext> deps = eventMap.get(dep.getRegexMatch());
        if (deps != null) {
          final Iterator<KafkaDependencyInstanceContext> i = deps.iterator();
          while (i.hasNext()) {
            final KafkaDependencyInstanceContext curr = i.next();
            if (curr == dep) {
              i.remove();
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
  public synchronized boolean removeList(String topic, String event,List<KafkaDependencyInstanceContext> list){
    System.out.printf("IN remove list\n");
    List<String> ori = new ArrayList<String>(this.topicEventMap.keySet());
    Map<String,List<KafkaDependencyInstanceContext>> eventMap = this.topicEventMap
        .get(topic);
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
    List<String> res = new ArrayList<String>(this.topicEventMap.keySet());
    return res==ori;
  }
}
