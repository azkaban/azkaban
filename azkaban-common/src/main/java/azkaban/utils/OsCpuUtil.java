/*
 * Copyright 2019 LinkedIn Corp.
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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for getting CPU usage (in percentage)
 *
 * CPU information is obtained from /proc/stat, so only Linux systems will support this class
 *
 * Assumes frequent calls at regular intervals to {@link #getCpuLoad() getCpuLoad}. The length of
 * time over which cpu load is calculated can be adjusted with parameter
 * {@code numCpuStatsToCollect} in the class constructor and how often {@link #getCpuLoad()
 * getCpuLoad} is called.
 * Example: if {@link #getCpuLoad() getCpuLoad} is called every second and {@code
 * numCpuStatsToCollect} is set to 60 then each call to {@link #getCpuLoad() getCpuLoad} returns
 * the cpu load over the last minute.
 */
public class OsCpuUtil {

  private static final Logger logger = LoggerFactory.getLogger(OsCpuUtil.class);

  private static final String CPU_STAT_FILE = "/proc/stat";

  private final Deque<CpuStats> collectedCpuStats;

  @Inject
  public OsCpuUtil(int numCpuStatsToCollect) {
    if (numCpuStatsToCollect <= 0) {
      numCpuStatsToCollect = 1;
    }
    this.collectedCpuStats = new ArrayDeque<>(numCpuStatsToCollect);

    final CpuStats cpuStats = getCpuStats();
    if (cpuStats != null) {
      for (int i = 0; i < numCpuStatsToCollect; i++) {
        this.collectedCpuStats.push(cpuStats);
      }
    }
  }

  /**
   * Collects a new cpu stat data point and calculates cpu load with it and the oldest one
   * collected which is then deleted.
   *
   * @return percentage of CPU usage. -1 if there are no cpu stats.
   */
  public double getCpuLoad() {
    if (this.collectedCpuStats.isEmpty()) {
      return -1;
    }
    final CpuStats oldestCpuStats = this.collectedCpuStats.pollLast();
    final CpuStats newestCpuStats = getCpuStats();
    this.collectedCpuStats.push(newestCpuStats);

    return calcCpuLoad(oldestCpuStats, newestCpuStats);
  }

  private double calcCpuLoad(final CpuStats startCpuStats, final CpuStats endCpuStats) {
    final long startSysUptime = startCpuStats.getSysUptime();
    final long startTimeCpuIdle = startCpuStats.getTimeCpuIdle();
    final long endSysUptime = endCpuStats.getSysUptime();
    final long endTimeCpuIdle = endCpuStats.getTimeCpuIdle();

    if (endSysUptime == startSysUptime) {
      logger.error("Failed to calculate cpu load: division by zero");
      return -1.0;
    }
    final double percentageCpuIdle =
        (100.0 * (endTimeCpuIdle - startTimeCpuIdle)) / (endSysUptime - startSysUptime);
    return 100.0 - percentageCpuIdle;
  }

  private CpuStats getCpuStats() {
    if (!Files.isRegularFile(Paths.get(CPU_STAT_FILE))) {
      // Mac doesn't use proc pseudo files for example.
      return null;
    }

    final String cpuLine = getCpuLineFromStatFile();
    if (cpuLine == null) {
      return null;
    }

    return getCpuStatsFromLine(cpuLine);
  }

  private String getCpuLineFromStatFile() {
    BufferedReader br = null;
    try {
      br = Files.newBufferedReader(Paths.get(CPU_STAT_FILE), StandardCharsets.UTF_8);
      String line;
      while ((line = br.readLine()) != null) {
        // looking for a line starting with "cpu<space>" which aggregates the values in all of
        // the other "cpuN" lines.
        if (line.startsWith("cpu ")) {
          return line;
        }
      }
    } catch (final IOException e) {
      final String errMsg = "Failed to read cpu stat file: " + CPU_STAT_FILE;
      logger.error(errMsg, e);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (final IOException e) {
          final String errMsg = "Failed to close cpu stat file: " + CPU_STAT_FILE;
          logger.error(errMsg, e);
        }
      }
    }
    return null;
  }

  /**
   * Parses cpu usage information from /proc/stat file.
   * Example of line expected with the meanings of the values below:
   * cpu  4705  356  584    3699   23    23     0       0     0          0
   * ---- user nice system idle iowait  irq  softirq steal guest guest_nice
   *
   * Method visible within the package for testing purposes.
   *
   * @param line the text containing cpu usage statistics
   * @return CpuStats object. null if there is an error.
   */
  CpuStats getCpuStatsFromLine(final String line) {
    try {
      final String[] cpuInfo = line.split("\\s+");
      final long user = Long.parseLong(cpuInfo[1]);
      final long nice = Long.parseLong(cpuInfo[2]);
      final long system = Long.parseLong(cpuInfo[3]);
      final long idle = Long.parseLong(cpuInfo[4]);
      final long iowait = Long.parseLong(cpuInfo[5]);
      final long irq = Long.parseLong(cpuInfo[6]);
      final long softirq = Long.parseLong(cpuInfo[7]);
      final long steal = Long.parseLong(cpuInfo[8]);

      // guest and guest_nice are counted on user and nice respectively, so don't add them
      final long totalCpuTime = user + nice + system + idle + iowait + irq + softirq + steal;

      final long idleCpuTime = idle + iowait;

      return new CpuStats(totalCpuTime, idleCpuTime);
    } catch (final NumberFormatException | ArrayIndexOutOfBoundsException e) {
      final String errMsg = "Failed to parse cpu stats from line: " + line;
      logger.error(errMsg, e);
    }
    return null;
  }

  static class CpuStats {

    private final long sysUptime;
    private final long timeCpuIdle;

    public CpuStats(final long sysUptime, final long timeCpuIdle) {
      this.sysUptime = sysUptime;
      this.timeCpuIdle = timeCpuIdle;
    }

    public long getSysUptime() {
      return this.sysUptime;
    }

    public long getTimeCpuIdle() {
      return this.timeCpuIdle;
    }
  }
}
