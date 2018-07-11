package trigger.kafka;

import org.apache.avro.generic.GenericRecord;
import trigger.kafka.matcher.DependencyMatcher;


public class FieldRegexKafkaDependencyMatcher implements DependencyMatcher {
  FieldRegexKafkaDependencyMatcher(){}
  public boolean isMatch(GenericRecord payload, String dep){
    return true;
  }
}
