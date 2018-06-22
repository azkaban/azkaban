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

package azkaban.execapp;


import static org.assertj.core.api.Assertions.assertThatThrownBy;

import azkaban.ConfigValidator;
import azkaban.Constants.ConfigurationKeys;
import azkaban.InvalidAzkabanConfigException;
import azkaban.utils.Props;
import org.junit.Test;

public class ExecutorConfigValidatorTest {

  @Test
  public void projectDirMaxSizeAsZero() {
    final Props props = new Props();
    props.put(ConfigurationKeys.PROJECT_DIR_MAX_SIZE, 0);
    final ConfigValidator configValidator = new ExecutorConfigValidator(props);
    assertThatThrownBy(() -> {
      configValidator.validate();
    }).isInstanceOf(
        InvalidAzkabanConfigException.class).hasMessage(ConfigurationKeys.PROJECT_DIR_MAX_SIZE +
        " must > 0");
  }

  @Test
  public void projectDirMaxSizeOverflow() {
    final Props props = new Props();
    props.put(ConfigurationKeys.PROJECT_DIR_MAX_SIZE, "99999999999999999999999999999999");
    final ConfigValidator configValidator = new ExecutorConfigValidator(props);
    assertThatThrownBy(() -> {
      configValidator.validate();
    }).isInstanceOf(
        InvalidAzkabanConfigException.class).hasMessage("Invalid azkaban config value for key : "
        + ConfigurationKeys.PROJECT_DIR_MAX_SIZE);
  }

  @Test
  public void projectDirCleanupStartThresholdNegative() {
    final Props props = new Props();
    props.put(ConfigurationKeys.PROJECT_DIR_CLEANUP_START_THRESHOLD, "-1");
    final ConfigValidator configValidator = new ExecutorConfigValidator(props);
    assertThatThrownBy(() -> {
      configValidator.validate();
    }).isInstanceOf(
        InvalidAzkabanConfigException.class).hasMessage(
        ConfigurationKeys.PROJECT_DIR_CLEANUP_START_THRESHOLD + " must >= 0 and <= 100");
  }

  @Test
  public void projectDirCleanupStartThresholdGreaterThanStopThreshold() {
    final Props props = new Props();
    props.put(ConfigurationKeys.PROJECT_DIR_CLEANUP_START_THRESHOLD, "50");
    props.put(ConfigurationKeys.PROJECT_DIR_CLEANUP_STOP_THRESHOLD, "60");
    final ConfigValidator configValidator = new ExecutorConfigValidator(props);
    assertThatThrownBy(() -> {
      configValidator.validate();
    }).isInstanceOf(InvalidAzkabanConfigException.class).hasMessage(
        ConfigurationKeys.PROJECT_DIR_CLEANUP_STOP_THRESHOLD + " must < " + ConfigurationKeys
            .PROJECT_DIR_CLEANUP_START_THRESHOLD);
  }

}
