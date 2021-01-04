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

package azkaban.jobtype.hiveutils;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

import static azkaban.security.commons.SecurityUtils.MAPREDUCE_JOB_CREDENTIALS_BINARY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEHISTORYFILELOC;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.SCRATCHDIR;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

/**
 * Guice-like module for creating a Hive instance. Easily turned back into a
 * full Guice module when we have need of it.
 */
class HiveQueryExecutorModule {
  private HiveConf hiveConf = null;
  private CliSessionState ss = null;

  HiveConf provideHiveConf() {
    if (this.hiveConf != null) {
      return this.hiveConf;
    } else {
      this.hiveConf = new HiveConf(SessionState.class);
    }

    troublesomeConfig(HIVEHISTORYFILELOC, hiveConf);
    troublesomeConfig(SCRATCHDIR, hiveConf);

    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      System.out.println("Setting hadoop tokens ... ");
      hiveConf.set(MAPREDUCE_JOB_CREDENTIALS_BINARY,
          System.getenv(HADOOP_TOKEN_FILE_LOCATION));
      System.setProperty(MAPREDUCE_JOB_CREDENTIALS_BINARY,
          System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }

    return hiveConf;
  }

  private void troublesomeConfig(HiveConf.ConfVars value, HiveConf hc) {
    System.out.println("Troublesome config " + value + " = "
        + HiveConf.getVar(hc, value));
  }

  CliSessionState provideCliSessionState() {
    if (ss != null) {
      return ss;
    }
    ss = new CliSessionState(provideHiveConf());
    SessionState.start(ss);
    return ss;
  }

  protected void configure() {
    /** Nothing to do **/
  }
}
