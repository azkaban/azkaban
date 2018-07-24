package trigger.kafka.matcher;

public interface DependencyMatcher<T> {
  /**
   * Determine whether the dependency condition is match.
   */
  public boolean isMatch(T payload);

}
