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
 *
 */

package azkaban.webapp;

import static azkaban.webapp.servlet.AbstractAzkabanServlet.jarVersion;

import azkaban.db.DatabaseOperator;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import com.google.inject.Inject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusService {

  private static final Logger log = LoggerFactory.getLogger(StatusService.class);

  private final ExecutorLoader executorLoader;
  private final DatabaseOperator dbOperator;

  @Inject
  public StatusService(final ExecutorLoader executorLoader, final DatabaseOperator dbOperator) {
    this.executorLoader = executorLoader;
    this.dbOperator = dbOperator;
  }

  public Status getStatus() {
    final String version = jarVersion == null ? "unknown" : jarVersion;
    final Runtime runtime = Runtime.getRuntime();
    final long usedMemory = runtime.totalMemory() - runtime.freeMemory();

    // Build the status object
    return new Status(version,
        usedMemory,
        runtime.maxMemory(),
        getDbStatus(),
        getActiveExecutors());
  }

  private Map<Integer, Executor> getActiveExecutors() {
    final Map<Integer, Executor> executorMap = new HashMap<>();
    try {
      final List<Executor> executors = this.executorLoader.fetchActiveExecutors();
      for (final Executor executor : executors) {
        executorMap.put(executor.getId(), executor);
      }
    } catch (final ExecutorManagerException e) {
      log.error("Fetching executors failed!", e);
    }
    return executorMap;
  }

  private boolean getDbStatus() {
    try {
      return this.dbOperator.query("SELECT 1", rs -> true);
    } catch (final SQLException e) {
      log.error("DB Error", e);
    }
    return false;
  }

}
