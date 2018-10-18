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

import azkaban.Constants.ConfigurationKeys;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.log4j.Logger;

/**
 * Loads & provides executors.
 */
@Singleton
public class ActiveExecutors {

  private static final Logger logger = Logger.getLogger(ExecutorManager.class);

  private volatile ImmutableSet<Executor> activeExecutors;
  private final Props azkProps;
  private final ExecutorLoader executorLoader;

  @Inject
  public ActiveExecutors(final Props azkProps, final ExecutorLoader executorLoader) {
    this.azkProps = azkProps;
    this.executorLoader = executorLoader;
  }

  /**
   * Loads executors. Can be also used to reload executors if there have been changes in the DB.
   *
   * @throws ExecutorManagerException if no active executors are found or if loading executors
   * fails.
   */
  public void setupExecutors() throws ExecutorManagerException {
    final ImmutableSet<Executor> newExecutors;
    if (ExecutorManager.isMultiExecutorMode(this.azkProps)) {
      newExecutors = setupMultiExecutors();
    } else {
      // TODO remove this - switch everything to use multi-executor mode
      newExecutors = setupSingleExecutor();
    }
    if (newExecutors.isEmpty()) {
      final String error = "No active executors found";
      logger.error(error);
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

  private ImmutableSet<Executor> setupMultiExecutors() throws ExecutorManagerException {
    logger.info("Initializing multi executors from database.");
    return ImmutableSet.copyOf(this.executorLoader.fetchActiveExecutors());
  }

  private ImmutableSet<Executor> setupSingleExecutor() throws ExecutorManagerException {
    if (this.azkProps.containsKey(ConfigurationKeys.EXECUTOR_PORT)) {
      return getOrAddSingleExecutor();
    } else {
      // throw exception when in single executor mode and no executor port specified in azkaban
      // properties
      //todo chengren311: convert to slf4j and parameterized logging
      final String error = "Missing" + ConfigurationKeys.EXECUTOR_PORT + " in azkaban properties.";
      logger.error(error);
      throw new ExecutorManagerException(error);
    }
  }

  private ImmutableSet<Executor> getOrAddSingleExecutor() throws ExecutorManagerException {
    // add single executor, if specified as per properties
    final String executorHost = this.azkProps
        .getString(ConfigurationKeys.EXECUTOR_HOST, "localhost");
    final int executorPort = this.azkProps.getInt(ConfigurationKeys.EXECUTOR_PORT);
    logger.info(String.format("Initializing single executor %s:%d", executorHost, executorPort));
    Executor executor = this.executorLoader.fetchExecutor(executorHost, executorPort);
    if (executor == null) {
      executor = this.executorLoader.addExecutor(executorHost, executorPort);
    } else if (!executor.isActive()) {
      executor.setActive(true);
      this.executorLoader.updateExecutor(executor);
    }
    return ImmutableSet.of(new Executor(executor.getId(), executorHost, executorPort, true));
  }

}
