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

package azkaban.flowtrigger.testplugin;

import azkaban.flowtrigger.DependencyInstanceCallback;
import azkaban.flowtrigger.DependencyInstanceConfig;
import azkaban.flowtrigger.DependencyInstanceContext;
import azkaban.flowtrigger.DependencyInstanceRuntimeProps;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("FutureReturnValueIgnored")
public class TestDependencyInstanceContext implements DependencyInstanceContext {

  private static final ScheduledExecutorService scheduleSerivce = Executors
      .newScheduledThreadPool(1);

  private final DependencyInstanceCallback callback;

  public TestDependencyInstanceContext(final DependencyInstanceConfig config,
      final DependencyInstanceRuntimeProps runtimeProps,
      final DependencyInstanceCallback callback) {
    final long expectedRunTime = Long.valueOf(config.get("runtime"));
    this.callback = callback;
    scheduleSerivce.schedule(this::onSuccess, expectedRunTime, TimeUnit.SECONDS);
  }

  private void onSuccess() {
    this.callback.onSuccess(this);
  }

  @Override
  public void cancel() {
    this.callback.onCancel(this);
  }
}
