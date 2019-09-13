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

package azkaban.execapp;

import azkaban.ConfigValidator;
import azkaban.Constants.ConfigurationKeys;
import azkaban.Constants.DefaultValue;
import azkaban.utils.Props;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Validator of Azkaban executor's configuration
 */

@Singleton
public class ExecutorConfigValidator extends ConfigValidator {

  @Inject
  public ExecutorConfigValidator(final Props azProps) {
    super(azProps);
  }

  /**
   * Validate azkaban executor's configuration
   *
   * @throws azkaban.InvalidAzkabanConfigException if configuration is invalid
   */
  @Override
  public void validate() {
    final long projectDirMaxSize = getLong(ConfigurationKeys.PROJECT_DIR_MAX_SIZE,
        DefaultValue.PROJECT_DIR_MAX_SIZE);

    final int projectDirStartDeletionThreshold = getInt(
        ConfigurationKeys.PROJECT_DIR_CLEANUP_START_THRESHOLD,
        DefaultValue.PROJECT_DIR_CLEANUP_START_THRESHOLD);

    final int projectDirStopDeletionThreshold = getInt(
        ConfigurationKeys.PROJECT_DIR_CLEANUP_STOP_THRESHOLD,
        DefaultValue.PROJECT_DIR_CLEANUP_STOP_THRESHOLD);

    check(projectDirMaxSize > 0, ConfigurationKeys.PROJECT_DIR_MAX_SIZE + " must > 0");
    check(projectDirStartDeletionThreshold >= 0 && projectDirStartDeletionThreshold <= 100,
        ConfigurationKeys.PROJECT_DIR_CLEANUP_START_THRESHOLD + " must >= 0 and <= 100");
    check(projectDirStopDeletionThreshold >= 0 && projectDirStopDeletionThreshold <= 100,
        ConfigurationKeys.PROJECT_DIR_CLEANUP_STOP_THRESHOLD + " must >= 0 and <= 100");
    check(projectDirStopDeletionThreshold < projectDirStartDeletionThreshold,
        ConfigurationKeys.PROJECT_DIR_CLEANUP_STOP_THRESHOLD + " must < " + ConfigurationKeys
            .PROJECT_DIR_CLEANUP_START_THRESHOLD);
  }
}
