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

package azkaban.dag;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Singleton;

/**
 * Thread safe and non blocking service for DAG processing.
 *
 * Since only one thread is used to progress the DAG, thread synchronization is avoided.
 */
@Singleton
public class DagService {

  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  public void startFlow(final Flow flow) {
    this.executorService.submit(() -> flow.start());
  }

  public void markJobSuccess(final Node node) {
    this.executorService.submit(() -> node.markSuccess());
  }

  public void failJob(final Node node) {
    this.executorService.submit(() -> node.markFailure());
  }

  public void killFlow(final Flow flow) {
    this.executorService.submit(() -> flow.kill());
  }

  public void unblockJob(final Node node) {

  }
}
