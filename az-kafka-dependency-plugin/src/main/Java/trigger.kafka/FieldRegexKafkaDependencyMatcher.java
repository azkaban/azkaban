package trigger.kafka;

import trigger.kafka.matcher.DependencyMatcher;


public class FieldRegexKafkaDependencyMatcher implements DependencyMatcher {
  FieldRegexKafkaDependencyMatcher() {
  }

  @Override
  public boolean isMatch(final String payload, final String dep) {
    return true;
  }
}
