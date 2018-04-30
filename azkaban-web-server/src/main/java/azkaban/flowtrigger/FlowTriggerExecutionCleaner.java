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

package azkaban.flowtrigger;

import azkaban.flowtrigger.database.FlowTriggerInstanceLoader;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

/**
 * This is to purge old flow trigger execution records from the db table.
 * Otherwise the table will keep growing indefinitely as triggers are executed, leading to
 * excessive query time on the table.
 * The cleanup policy is removing trigger instances finishing older than 20 days back.
 */
@SuppressWarnings("FutureReturnValueIgnored")
public class FlowTriggerExecutionCleaner {

  private static final Duration CLEAN_INTERVAL = Duration.ofMinutes(10);
  private static final Duration RETENTION_PERIOD = Duration.ofDays(10);
  private final ScheduledExecutorService scheduler;
  private final FlowTriggerInstanceLoader flowTriggerInstanceLoader;

  @Inject
  public FlowTriggerExecutionCleaner(final FlowTriggerInstanceLoader loader) {
    this.flowTriggerInstanceLoader = loader;
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  public void start() {
    this.scheduler.scheduleAtFixedRate(() -> {
      FlowTriggerExecutionCleaner.this.flowTriggerInstanceLoader
          .deleteTriggerExecutionsFinishingOlderThan(System
              .currentTimeMillis() - RETENTION_PERIOD.toMillis());
    }, 0, CLEAN_INTERVAL.getSeconds(), TimeUnit.SECONDS);
  }

  public void shutdown() {
    this.scheduler.shutdown();
    this.scheduler.shutdownNow();
  }
}
