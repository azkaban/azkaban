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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import org.junit.Test;

@SuppressWarnings("FutureReturnValueIgnored")
public class ExecutorServiceUtilsTest {

  private final ExecutorServiceUtils executorServiceUtils = new ExecutorServiceUtils();

  @Test
  public void gracefulShutdown() throws InterruptedException {
    // given
    final ExecutorService service = Executors.newSingleThreadExecutor();

    // when
    this.executorServiceUtils.gracefulShutdown(service, Duration.ofMillis(1));

    // then
    assertThat(service.isShutdown()).isTrue();
  }

  @Test
  public void force_shutdown_after_timeout() throws InterruptedException {
    // given
    final ExecutorService service = Executors.newSingleThreadExecutor();

    // when
    service.submit(this::sleep);
    final long beginShutdownTime = System.currentTimeMillis();
    this.executorServiceUtils.gracefulShutdown(service, Duration.ofMillis(1));
    final long endShutdownTime = System.currentTimeMillis();

    // then
    assertThat(service.isShutdown()).isTrue();
    final long shutdownDuration = endShutdownTime - beginShutdownTime;
    // Give some buffer for overhead to reduce false positives.
    assertThat(shutdownDuration).isLessThan(10);
  }

  @Test
  public void can_not_submit_tasks_after_shutdown() throws
      InterruptedException {
    // given
    final ExecutorService service = Executors.newSingleThreadExecutor();
    service.submit(this::sleep);

    // when
    this.executorServiceUtils.gracefulShutdown(service, Duration.ofMillis(1));
    final Throwable thrown = catchThrowable(() -> {
      service.submit(this::sleep);
    });

    // then
    assertThat(thrown).isInstanceOf(RejectedExecutionException.class);
  }

  private void sleep() {
    try {
      Thread.sleep(1000);
    } catch (final InterruptedException ex) {
    }
  }
}
