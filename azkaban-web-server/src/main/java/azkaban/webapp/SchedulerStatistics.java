/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.webapp;

import java.io.File;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.scheduler.Schedule;
import azkaban.scheduler.ScheduleManager;
import azkaban.scheduler.ScheduleStatisticManager;
import azkaban.scheduler.ScheduleManagerException;

public class SchedulerStatistics {
  public static Map<String, Object> getStatistics(
      int scheduleId, AzkabanWebServer server)
      throws ScheduleManagerException {
    if (ScheduleStatisticManager.getCacheDirectory() == null) {
      ScheduleStatisticManager.setCacheFolder(
          new File(server.getServerProps().getString("cache.directory", "cache")));
    }
    Map<String, Object> data = ScheduleStatisticManager.loadCache(scheduleId);
    if (data != null) {
      return data;
    }

    // Calculate data and cache it
    data = calculateStats(scheduleId, server);
    ScheduleStatisticManager.saveCache(scheduleId, data);
    return data;
  }

  private static Map<String, Object> calculateStats(
      int scheduleId, AzkabanWebServer server)
      throws ScheduleManagerException {
    Map<String, Object> data = new HashMap<String, Object>();
    ExecutorManagerAdapter executorManager = server.getExecutorManager();
    ScheduleManager scheduleManager = server.getScheduleManager();
    Schedule schedule = scheduleManager.getSchedule(scheduleId);

    try {
      List<ExecutableFlow> executables = executorManager.getExecutableFlows(
          schedule.getProjectId(), schedule.getFlowName(), 0,
          ScheduleStatisticManager.STAT_NUMBERS, Status.SUCCEEDED);

      long average = 0;
      long min = Integer.MAX_VALUE;
      long max = 0;
      if (executables.isEmpty()) {
        average = 0;
        min = 0;
        max = 0;
      } else {
        for (ExecutableFlow flow : executables) {
          long time = flow.getEndTime() - flow.getStartTime();
          average += time;
          if (time < min) {
            min = time;
          }
          if (time > max) {
            max = time;
          }
        }
        average /= executables.size();
      }

      data.put("average", average);
      data.put("min", min);
      data.put("max", max);
    } catch (ExecutorManagerException e) {
      e.printStackTrace();
    }

    return data;
  }
}
