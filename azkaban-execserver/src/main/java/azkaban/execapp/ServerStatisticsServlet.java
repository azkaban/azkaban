/*
 * Copyright 2015 LinkedIn Corp.
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

package azkaban.execapp;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.executor.ExecutorInfo;
import azkaban.utils.JSONUtils;


public class ServerStatisticsServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;
  private static final int cacheTimeInMilliseconds = 1000;
  private static final Logger logger = Logger.getLogger(ServerStatisticsServlet.class);
  private static final String noCacheParamName = "nocache";
  private static final boolean exists_Bash = new File("/bin/bash").exists();
  private static final boolean exists_Cat = new File("/bin/cat").exists();
  private static final boolean exists_Grep = new File("/bin/grep").exists();
  private static final boolean exists_Meminfo = new File("/proc/meminfo").exists();
  private static final boolean exists_LoadAvg = new File("/proc/loadavg").exists();

  protected static long lastRefreshedTime = 0;
  protected static ExecutorInfo cachedstats = null;

  /**
   * Handle all get request to Statistics Servlet {@inheritDoc}
   *
   * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest,
   *      javax.servlet.http.HttpServletResponse)
   */
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

    boolean noCache = null != req && Boolean.valueOf(req.getParameter(noCacheParamName));

    if (noCache || System.currentTimeMillis() - lastRefreshedTime > cacheTimeInMilliseconds) {
      this.populateStatistics(noCache);
    }

    JSONUtils.toJSON(cachedstats, resp.getOutputStream(), true);
  }

  /**
   * fill the result set with the percent of the remaining system memory on the server.
   * @param stats reference to the result container which contains all the results, this specific method
   *              will only work work on the property "remainingMemory" and "remainingMemoryPercent".
   *
   * NOTE:
   * a double value will be used to present the remaining memory,
   *         a returning value of '55.6' means 55.6%
   */
  protected void fillRemainingMemoryPercent(ExecutorInfo stats) {
    if (exists_Bash && exists_Cat && exists_Grep && exists_Meminfo) {
      java.lang.ProcessBuilder processBuilder =
          new java.lang.ProcessBuilder("/bin/bash", "-c",
              "/bin/cat /proc/meminfo | grep -E \"^MemTotal:|^MemFree:|^Buffers:|^Cached:|^SwapCached:\"");
      try {
        ArrayList<String> output = new ArrayList<String>();
        Process process = processBuilder.start();
        process.waitFor();
        InputStream inputStream = process.getInputStream();
        try {
          java.io.BufferedReader reader = new java.io.BufferedReader(new InputStreamReader(inputStream));
          String line = null;
          while ((line = reader.readLine()) != null) {
            output.add(line);
          }
        } finally {
          inputStream.close();
        }

        long totalMemory = 0;
        long totalFreeMemory = 0;
        Long parsedResult = (long) 0;

        // process the output from bash call.
        // we expect the result from the bash call to be something like following -
        // MemTotal:       65894264 kB
        // MemFree:        57753844 kB
        // Buffers:          305552 kB
        // Cached:          3802432 kB
        // SwapCached:            0 kB
        // Note : total free memory = freeMemory + cached + buffers + swapCached
        // TODO : think about merging the logic in systemMemoryInfo as the logic is similar
        if (output.size() == 5) {
          for (String result : output) {
            // find the total memory and value the variable.
            parsedResult = extractMemoryInfo("MemTotal", result);
            if (null != parsedResult) {
              totalMemory = parsedResult;
              continue;
            }

            // find the free memory.
            parsedResult = extractMemoryInfo("MemFree", result);
            if (null != parsedResult) {
              totalFreeMemory += parsedResult;
              continue;
            }

            // find the Buffers.
            parsedResult = extractMemoryInfo("Buffers", result);
            if (null != parsedResult) {
              totalFreeMemory += parsedResult;
              continue;
            }

            // find the Cached.
            parsedResult = extractMemoryInfo("SwapCached", result);
            if (null != parsedResult) {
              totalFreeMemory += parsedResult;
              continue;
            }

            // find the Cached.
            parsedResult = extractMemoryInfo("Cached", result);
            if (null != parsedResult) {
              totalFreeMemory += parsedResult;
              continue;
            }
          }
        } else {
          logger.error("failed to get total/free memory info as the bash call returned invalid result."
              + String.format(" Output from the bash call - %s ", output.toString()));
        }

        // the number got from the proc file is in KBs we want to see the number in MBs so we are dividing it by 1024.
        stats.setRemainingMemoryInMB(totalFreeMemory / 1024);
        stats.setRemainingMemoryPercent(totalMemory == 0 ? 0 : ((double) totalFreeMemory / (double) totalMemory) * 100);
      } catch (Exception ex) {
        logger.error("failed fetch system memory info "
            + "as exception is captured when fetching result from bash call. Ex -" + ex.getMessage());
      }
    } else {
      logger.error("failed fetch system memory info, one or more files from the following list are missing -  "
          + "'/bin/bash'," + "'/bin/cat'," + "'/proc/loadavg'");
    }
  }

  private Long extractMemoryInfo(String field, String result) {
    Long returnResult = null;
    if (null != result && null != field && result.matches(String.format("^%s:.*", field))
        && result.split("\\s+").length > 2) {
      try {
        returnResult = Long.parseLong(result.split("\\s+")[1]);
        logger.debug(field + ":" + returnResult);
      } catch (NumberFormatException e) {
        returnResult = 0L;
        logger.error(String.format("yielding 0 for %s as output is invalid - %s", field, result));
      }
    }
    return returnResult;
  }

  /**
   * call the data providers to fill the returning data container for statistics data.
   * This function refreshes the static cached copy of data in case if necessary.
   * */
  protected synchronized void populateStatistics(boolean noCache) {
    //check again before starting the work.
    if (noCache || System.currentTimeMillis() - lastRefreshedTime > cacheTimeInMilliseconds) {
      final ExecutorInfo stats = new ExecutorInfo();

      fillRemainingMemoryPercent(stats);
      fillRemainingFlowCapacityAndLastDispatchedTime(stats);
      fillCpuUsage(stats);

      cachedstats = stats;
      lastRefreshedTime = System.currentTimeMillis();
    }
  }

  /**
   * fill the result set with the remaining flow capacity .
   * @param stats reference to the result container which contains all the results, this specific method
   *              will only work on the property "remainingFlowCapacity".
   */
  protected void fillRemainingFlowCapacityAndLastDispatchedTime(ExecutorInfo stats) {

    AzkabanExecutorServer server = AzkabanExecutorServer.getApp();
    if (server != null) {
      FlowRunnerManager runnerMgr = AzkabanExecutorServer.getApp().getFlowRunnerManager();
      int assignedFlows = runnerMgr.getNumRunningFlows() + runnerMgr.getNumQueuedFlows();
      stats.setRemainingFlowCapacity(runnerMgr.getMaxNumRunningFlows() - assignedFlows);
      stats.setNumberOfAssignedFlows(assignedFlows);
      stats.setLastDispatchedTime(runnerMgr.getLastFlowSubmittedTime());
    } else {
      logger.error("failed to get data for remaining flow capacity or LastDispatchedTime"
          + " as the AzkabanExecutorServer has yet been initialized.");
    }
  }

  /**<pre>
   * fill the result set with the CPU usage .
   * Note : As the 'Top' bash call doesn't yield accurate result for the system load,
   *        the implementation has been changed to load from the "proc/loadavg" which keeps
   *        the moving average of the system load, we are pulling the average for the recent 1 min.
   *</pre>
   * @param stats reference to the result container which contains all the results, this specific method
   *              will only work on the property "cpuUsage".
   */
  protected void fillCpuUsage(ExecutorInfo stats) {
    if (exists_Bash && exists_Cat && exists_LoadAvg) {
      java.lang.ProcessBuilder processBuilder =
          new java.lang.ProcessBuilder("/bin/bash", "-c", "/bin/cat /proc/loadavg");
      try {
        ArrayList<String> output = new ArrayList<String>();
        Process process = processBuilder.start();
        process.waitFor();
        InputStream inputStream = process.getInputStream();
        try {
          java.io.BufferedReader reader = new java.io.BufferedReader(new InputStreamReader(inputStream));
          String line = null;
          while ((line = reader.readLine()) != null) {
            output.add(line);
          }
        } finally {
          inputStream.close();
        }

        // process the output from bash call.
        if (output.size() > 0) {
          String[] splitedresult = output.get(0).split("\\s+");
          double cpuUsage = 0.0;

          try {
            cpuUsage = Double.parseDouble(splitedresult[0]);
          } catch (NumberFormatException e) {
            logger.error("yielding 0.0 for CPU usage as output is invalid -" + output.get(0));
          }
          logger.info("System load : " + cpuUsage);
          stats.setCpuUpsage(cpuUsage);
        }
      } catch (Exception ex) {
        logger.error("failed fetch system load info "
            + "as exception is captured when fetching result from bash call. Ex -" + ex.getMessage());
      }
    } else {
      logger.error("failed fetch system load info, one or more files from the following list are missing -  "
          + "'/bin/bash'," + "'/bin/cat'," + "'/proc/loadavg'");
    }
  }
}
