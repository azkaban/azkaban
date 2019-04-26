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
package azkaban.executor;

import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Loads & provides executors.
 */
@Singleton
public class ActiveExecutors {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorManager.class);

  private volatile ImmutableSet<Executor> activeExecutors;
  private final ExecutorLoader executorLoader;

  @Inject
  public ActiveExecutors(final ExecutorLoader executorLoader) {
    this.executorLoader = executorLoader;
  }

  /**
   * Loads executors. Can be also used to reload executors if there have been changes in the DB.
   *
   * @throws ExecutorManagerException if no active executors are found or if loading executors
   * fails.
   */
  public void setupExecutors() throws ExecutorManagerException {
    final ImmutableSet<Executor> newExecutors = loadExecutors();
    if (newExecutors.isEmpty()) {
      final String error = "No active executors found";
      LOG.error(error);
      throw new ExecutorManagerException(error);
    } else {
      this.activeExecutors = newExecutors;
    }
  }

  /**
   * Returns all executors. The result is cached. To reload, call {@link #setupExecutors()}.
   *
   * @return all executors
   */
  public Collection<Executor> getAll() {
    return this.activeExecutors;
  }

  private ImmutableSet<Executor> loadExecutors() throws ExecutorManagerException {
    LOG.info("Initializing executors from database.");
    return ImmutableSet.copyOf(this.executorLoader.fetchActiveExecutors());
  }
}
