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

package azkaban.utils;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor service related utilities.
 */
public class ExecutorServiceUtils {

  private static final Logger logger = LoggerFactory.getLogger(ExecutorServiceUtils.class);
  private static final TimeUnit MILLI_SECONDS_TIME_UNIT = TimeUnit.MILLISECONDS;

  /**
   * Gracefully shuts down the given executor service.
   *
   * <p>Adopted from
   * <a href="https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html">
   * the Oracle JAVA Documentation.
   * </a>
   *
   * @param service the service to shutdown
   * @param timeout max wait time for the tasks to shutdown. Note that the max wait time is 2
   *     times this value due to the two stages shutdown strategy.
   * @throws InterruptedException if the current thread is interrupted
   */
  public void gracefulShutdown(final ExecutorService service, final Duration timeout)
      throws InterruptedException {
    service.shutdown(); // Disable new tasks from being submitted
    final long timeout_in_unit_of_miliseconds = timeout.toMillis();
    // Wait a while for existing tasks to terminate
    if (!service.awaitTermination(timeout_in_unit_of_miliseconds, MILLI_SECONDS_TIME_UNIT)) {
      service.shutdownNow(); // Cancel currently executing tasks
      // Wait a while for tasks to respond to being cancelled
      if (!service.awaitTermination(timeout_in_unit_of_miliseconds, MILLI_SECONDS_TIME_UNIT)) {
        logger.error("The executor service did not terminate.");
      }
    }
  }
}
