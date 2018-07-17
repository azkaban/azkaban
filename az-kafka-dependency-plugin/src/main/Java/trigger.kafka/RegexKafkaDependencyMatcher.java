package trigger.kafka;

import java.util.regex.Pattern;
import trigger.kafka.matcher.DependencyMatcher;


public class RegexKafkaDependencyMatcher implements DependencyMatcher {
  RegexKafkaDependencyMatcher() {
  }

  @Override
  public boolean isMatch(final String payload, final String rule) {
    return Pattern.compile(rule).matcher(payload.toString()).find();
  }
}
