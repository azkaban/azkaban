/*
 * Copyright 2018 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package trigger.kafka;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;


/**
 * A map data structure that enables efficient lookup by topic and adding/removing topic event pairs.
 * Structure looks like:
 * {
 *  -Topic1:{
 *  ----Rule1
 *  ---------[List of dependencies]
 *  ----Rule2
 *  ---------[List of dependencies]
 *  }
 *  -Topic2:{
 *  ----Rule1
 *  ---------[List of dependencies]
 *  ----Rule2
 *  ---------[List of dependencies]
 *  }
 * }
 *
 */
public class KafkaDepInstanceCollection {

  private final Map<String, Map<String, List<KafkaDependencyInstanceContext>>> topicEventMap;

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

  public boolean hasTopic(final String topic) {
    return !(this.topicEventMap.get(topic) == null);
  }

  /**
   * Get a list of topics.
   * @return List of String of topics
   */
  public synchronized List<String> getTopicList() {
    final List<String> res = new ArrayList<>(this.topicEventMap.keySet());
    return res;
  }

  /**
   * Return a set of pattern that matches with the payload.
   * @param payload and topic
   * @return regexs that meet the customized requirement
   */
  public synchronized Set<String> regexInTopic(final String topic, final String payload) {
    final Set<String> res = new HashSet<>();
    final Map<String, List<KafkaDependencyInstanceContext>> eventMap = this.topicEventMap.get(topic);
    if (eventMap == null) {
      return Collections.emptySet();
    }

    for (final Map.Entry<String, List<KafkaDependencyInstanceContext>> entry : eventMap.entrySet()) {
      final RegexKafkaDependencyMatcher matcher = new RegexKafkaDependencyMatcher(Pattern.compile(entry.getKey()));
      if (matcher.isMatch(payload)) {
        res.add(entry.getKey());
      }
    }
    return res;
  }

  /**
   * Returns dependencies with topic and dependency's event regular expression match
   */
  public synchronized List<KafkaDependencyInstanceContext> getDepsByTopicAndEvent(final String topic,
      final String regex) {
    final Map<String, List<KafkaDependencyInstanceContext>> regexMap = this.topicEventMap.get(topic);
    if (regexMap != null) {
      return regexMap.get(regex);
    }
    return Collections.emptyList();
  }

  public synchronized void remove(final KafkaDependencyInstanceContext dep) {
    final Map<String, List<KafkaDependencyInstanceContext>> regexMap = this.topicEventMap.get(dep.getTopicName());
    if (regexMap != null) {
      final List<KafkaDependencyInstanceContext> deps = regexMap.get(dep.getRegexMatch());
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
          regexMap.remove(dep.getRegexMatch());
        }
        if (regexMap.isEmpty()) {
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
