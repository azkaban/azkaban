/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban;

import static java.util.Objects.requireNonNull;

import azkaban.utils.Props;

/**
 * Validator of Azkaban configuration.
 * Inheriting class will need to implement its own validation logic.
 * E.g: azkaban executor will need to validate executor config.
 */
public abstract class ConfigValidator {

  protected final Props props;

  public ConfigValidator(final Props azProps) {
    this.props = requireNonNull(azProps, "azProps cannot be null");
  }

  /**
   * This method is adapted from
   * {@link com.google.common.base.Preconditions#checkArgument(boolean, Object)}
   *
   * Ensures the truth of an expression involving one or more parameters to the calling method.
   *
   * @param expression a boolean expression
   * @param errorMessage the exception message to use if the check fails;
   * @throws InvalidAzkabanConfigException if {@code expression} is false
   */
  protected final void check(final boolean expression, final String errorMessage) {
    if (!expression) {
      throw new InvalidAzkabanConfigException(errorMessage);
    }
  }

  /**
   * Returns int representation of the value.
   *
   * @param configKey the config key
   * @param defaultValue default value to return if value is null;
   * @return the value of the {@param configKey}
   * @throws InvalidAzkabanConfigException if value is non-int.
   */
  protected final int getInt(final String configKey, final int defaultValue) {
    try {
      final int val = this.props.getInt(configKey, defaultValue);
      return val;
    } catch (final NumberFormatException e) {
      throw new InvalidAzkabanConfigException(String.format("Invalid azkaban config value for key"
          + " : %s", configKey));
    }
  }

  /**
   * Returns long representation of the value.
   *
   * @param configKey the config key
   * @param defaultValue default value to return if value is null;
   * @return the value of the {@param configKey}
   * @throws InvalidAzkabanConfigException if value is non-int.
   */
  protected final long getLong(final String configKey, final long defaultValue) {
    try {
      final long val = this.props.getLong(configKey, defaultValue);
      return val;
    } catch (final NumberFormatException e) {
      throw new InvalidAzkabanConfigException(String.format("Invalid azkaban config value for key"
          + " : %s", configKey));
    }
  }

  /**
   * Validate azkaban executor's configuration
   *
   * @throws InvalidAzkabanConfigException if configuration is invalid
   */
  abstract public void validate();
}
