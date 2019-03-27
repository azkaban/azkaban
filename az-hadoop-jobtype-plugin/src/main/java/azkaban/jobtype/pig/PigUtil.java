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

package azkaban.jobtype.pig;

import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.mapred.Counters;
import org.apache.pig.PigRunner;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;

import azkaban.jobtype.AzkabanPigListener;
import azkaban.utils.Props;

/**
 * This class contains utility methods for pig jobs.
 */
public class PigUtil {
  private static final String PIG_DUMP_HADOOP_COUNTER_PROPERTY = "pig.dump.hadoopCounter";

  /**
   * Runs pig job using PigRunner
   * @param args Arguments to be passed to pig runner
   * @param props Azkaban props object
   * @return
   * @throws Exception
   */
  public static PigStats runPigJob(String[] args, Props props) throws Exception {
    PigStats stats = null;
    if (props.getBoolean("pig.listener.visualizer", false) == true) {
      stats = PigRunner.run(args, new AzkabanPigListener(props));
    } else {
      stats = PigRunner.run(args, null);
    }
    return stats;
  }

  /**
   * Dump Hadoop counters for each of the M/R jobs in the given PigStats.
   *
   * @param pigStats PigStats object returned by PigRunner run method after running the pig job
   * @param props Azkaban Props Object
   */
  public static void dumpHadoopCounters(PigStats pigStats, Props props) {
    try {
      if (props.getBoolean(PIG_DUMP_HADOOP_COUNTER_PROPERTY, false)) {
        if (pigStats != null) {
          JobGraph jGraph = pigStats.getJobGraph();
          Iterator<JobStats> iter = jGraph.iterator();
          while (iter.hasNext()) {
            JobStats jobStats = iter.next();
            System.out.println("\n === Counters for job: " + jobStats.getJobId() + " ===");
            Counters counters = jobStats.getHadoopCounters();
            if (counters != null) {
              for (Counters.Group group : counters) {
                System.out.println(" Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
                System.out.println("  number of counters in this group: " + group.size());
                for (Counters.Counter counter : group) {
                  System.out.println("  - " + counter.getDisplayName() + ": " + counter.getCounter());
                }
              }
            } else {
              System.out.println("There are no counters");
            }
          }
        } else {
          System.out.println("pigStats is null, can't dump Hadoop counters");
        }
      }
    } catch (Exception e) {
      System.out.println("Unexpected error: " + e.getMessage());
      e.printStackTrace(System.out);
    }
  }

  /**
   * Utility method for killing self.
   */
  public static void selfKill() {
    // see jira ticket PIG-3313. Will remove these when we use pig binary with
    // that patch.
    // /////////////////////
    System.out.println("Trying to do self kill, in case pig could not.");
    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    Thread[] threadArray = threadSet.toArray(new Thread[threadSet.size()]);
    for (Thread t : threadArray) {
      if (!t.isDaemon() && !t.equals(Thread.currentThread())) {
        System.out.println("Killing thread " + t);
        t.stop();
      }
    }
    System.exit(1);
    // ////////////////////
    throw new RuntimeException("Pig job failed.");

  }
}
