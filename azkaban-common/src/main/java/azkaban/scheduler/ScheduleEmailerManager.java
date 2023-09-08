/*
 * Copyright 2023 LinkedIn Corp.
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

import azkaban.Constants;
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.metrics.MetricsManager;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.DaemonThreadFactory;
import azkaban.utils.Emailer;
import azkaban.utils.Props;
import azkaban.utils.TimeUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class is to provide a failure recover manager for schedules.
 * Schedule might be missed to execute, schedule might be deleted, and this class provide a generic way to handle
 * the notification to users by email.
 * */
@Singleton
public abstract class ScheduleEmailerManager {
  protected final Logger log;

  protected ExecutorService executor;
  // TODO: Refactored to AlerterHolder to support Iris use case
  protected final Emailer emailer;

  // parameters to adjust the thread pool size
  protected final int threadPoolSize;
  private final int DEFAULT_THREAD_POOL_SIZE = 5;
  private final String threadPoolName;

  public ScheduleEmailerManager(final Props azkProps,
      final Emailer emailer,
      final String threadPoolName) {
    this.log = LoggerFactory.getLogger(getClass().getSimpleName());
    this.emailer = emailer;
    this.threadPoolSize = azkProps.getInt(Constants.MISSED_SCHEDULE_THREAD_POOL_SIZE, this.DEFAULT_THREAD_POOL_SIZE);
    this.threadPoolName = threadPoolName;
  }

  protected abstract boolean isEmailerManagerEnabled();

  public void start() {
    // Create the thread pool
    this.executor =
        isEmailerManagerEnabled() ? Executors.newFixedThreadPool(this.threadPoolSize,
            new DaemonThreadFactory(this.threadPoolName)) : null;
    if (isEmailerManagerEnabled()) {
      log.info("{} is ready to take tasks.", getClass().getSimpleName());
    } else {
      log.info("{} is disabled.", getClass().getSimpleName());
    }
  }

  public boolean stop() throws InterruptedException {
    if (this.executor != null) {
      this.executor.shutdown();
      if (!this.executor.awaitTermination(30, TimeUnit.SECONDS)) {
        this.executor.shutdownNow();
      }
    }
    return true;
  }
}