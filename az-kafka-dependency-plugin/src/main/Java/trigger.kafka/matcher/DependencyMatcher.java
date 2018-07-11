package trigger.kafka.matcher;

import org.apache.avro.generic.GenericRecord;

public interface DependencyMatcher {
  /**
   * Determine whether the dependency condition is match.
   */
  public boolean isMatch(GenericRecord payload, String rule);
}
