package trigger.kafka;

import java.util.regex.Pattern;
import org.apache.avro.generic.GenericRecord;
import trigger.kafka.matcher.DependencyMatcher;


public class RegexKafkaDependencyMatcher implements DependencyMatcher {
  RegexKafkaDependencyMatcher(){}
  public boolean isMatch(GenericRecord payload, String rule){
    System.out.println("~~~~~~~~~~~~~~~~`In is match`~~~~~~~~~~~~~");
    System.out.println(payload);
    System.out.println(rule);
    System.out.println(Pattern.compile(rule).matcher(payload.toString()).find());
    System.out.println("~~~~~~~~~~~~~~~~`Out is match`~~~~~~~~~~~~~");
    return Pattern.compile(rule).matcher(payload.toString()).find();
  }
}
