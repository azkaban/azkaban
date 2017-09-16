package azkaban.utils;

import java.util.function.Function;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

/**
 * Utility Matcher to allow matching with a lambda function.
 *
 * @param <T> type of the matched object.
 */
public class LambdaMatcher<T> extends BaseMatcher<T> {

  private final Function<T, Boolean> matchFunction;
  private final String description;

  private LambdaMatcher(final Function<T, Boolean> matchFunction, final String description) {
    this.matchFunction = matchFunction;
    this.description = description;
  }

  public static <U> LambdaMatcher<U> matches(final Function<U, Boolean> matchFunction,
      final String description) {
    return new LambdaMatcher<>(matchFunction, description);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean matches(final Object o) {
    final T value;
    try {
      value = (T) o;
    } catch (final ClassCastException e) {
      return false;
    }
    return this.matchFunction.apply(value);
  }

  @Override
  public void describeTo(final Description description) {
    description.appendText(this.description);
  }

}
