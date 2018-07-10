package trigger.kafka;

import org.apache.avro.generic.GenericRecord;
import trigger.kafka.matcher.DependencyMatcher;


public class RegexKafkaDependencyMatcher implements DependencyMatcher {
  RegexKafkaDependencyMatcher(){}
  public boolean isMatch(GenericRecord payload, KafkaDependencyInstanceContext dep){
    return true;
  }
}
