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

package azkaban.scheduler;

import java.io.Serializable;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

public abstract class AbstractQuartzJob implements Job {

  /**
   * Cast the object to the original one with the type. The object must extend Serializable.
   */
  protected static <T extends Serializable> T asT(final Object service, final Class<T> type) {
    return type.cast(service);
  }

  @Override
  public abstract void execute(JobExecutionContext context);

  protected Object getKey(final JobExecutionContext context, final String key) {
    return context.getMergedJobDataMap().get(key);
  }
}
