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
import java.io.Serializable;
import java.util.TimeZone;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * FlowTriggerSchedule is the logical representation of a cron-based schedule. It couldn't be
 * changed once gets constructed. It will be used to schedule a trigger.
 */
public class CronSchedule implements Serializable {

  /**
   * CAUTION : Please do NOT change this serialVersionUID as it may break
   * backward compatibility.
   */
  private static final long serialVersionUID = -1330280892166841227L;
  private static final String DEFAULT_TIMEZONE = TimeZone.getDefault().getID();
  private final String cronExpression;
  private final String timeZone;

  /**
   * @throws IllegalArgumentException if cronExpression is null or blank
   */
  public CronSchedule(final String cronExpression) {
    this(cronExpression, DEFAULT_TIMEZONE);
  }

  /**
   * @throws IllegalArgumentException if cronExpression is null or blank
   */
  public CronSchedule(final String cronExpression, String timeZone) {
    Preconditions.checkArgument(StringUtils.isNotBlank(cronExpression));
    this.cronExpression = cronExpression;
    //todo chengren311: check cronExpression is valid: quartz has CronExpression.isValidExpression()
    this.timeZone = timeZone;
  }

  public String getCronExpression() {
    return this.cronExpression;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final CronSchedule that = (CronSchedule) o;

    return new EqualsBuilder()
        .append(this.cronExpression, that.cronExpression)
        .append(this.getTimeZone(), that.getTimeZone())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(this.cronExpression)
        .toHashCode();
  }

  public String getTimeZone() {
    if (null == timeZone) {
      return DEFAULT_TIMEZONE;
    }
    return timeZone;
  }

}
