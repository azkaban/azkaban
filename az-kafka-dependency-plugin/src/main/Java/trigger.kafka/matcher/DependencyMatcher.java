package trigger.kafka.matcher;

import org.apache.avro.generic.GenericRecord;
import trigger.kafka.KafkaDependencyInstanceContext;

public interface DependencyMatcher {
  /**
   * Determine whether the dependency condition is match.
   */
  public boolean isMatch(GenericRecord payload, KafkaDependencyInstanceContext dep);
}
