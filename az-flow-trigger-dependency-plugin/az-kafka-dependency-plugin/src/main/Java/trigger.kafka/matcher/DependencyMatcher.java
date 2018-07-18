package trigger.kafka.matcher;

public interface DependencyMatcher {
  /**
   * Determine whether the dependency condition is match.
   */
  public boolean isMatch(String payload, String rule);
}
