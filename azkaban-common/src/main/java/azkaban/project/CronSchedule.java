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

package azkaban.project;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

/**
 * FlowTriggerSchedule is the logical representation of a cron-based schedule.
 * It couldn't be changed once gets constructed.
 * It will be used to schedule a trigger.
 */
public class CronSchedule {

  private final String cronExpression;

  /**
   * @throws IllegalArgumentException if cronExpression is null or blank
   */
  public CronSchedule(final String cronExpression) {
    Preconditions.checkArgument(StringUtils.isNotBlank(cronExpression));
    this.cronExpression = cronExpression;
    //todo chengren311: check cronExpression is valid: quartz has CronExpression.isValidExpression()
  }

  public String getCronExpression() {
    return this.cronExpression;
  }
}
