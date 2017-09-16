/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
