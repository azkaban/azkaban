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

import com.google.inject.Injector;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.quartz.Job;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

/**
 * Produce Guice-able Job in this custom defined Job Factory.
 *
 * In order to allow Quaratz jobs easily inject dependency, we create this factory. Every Quartz job
 * will be constructed by newJob method.
 */
@Singleton
public class SchedulerJobFactory implements JobFactory {

  private final Injector injector;

  @Inject
  public SchedulerJobFactory(final Injector injector) {
    this.injector = injector;
  }

  @Override
  public Job newJob(final TriggerFiredBundle bundle, final Scheduler scheduler)
      throws SchedulerException {
    return (Job) this.injector.getInstance(bundle.getJobDetail()
        .getJobClass());
  }
}
