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

package azkaban.jobtype;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.pig.tools.pigstats.PigStats;

import azkaban.jobtype.pig.PigCommonConstants;
import azkaban.jobtype.pig.PigUtil;
import azkaban.utils.Props;


public class HadoopSecurePigWrapper {

  private static File pigLogFile;

  private static Props props;

  private static final Logger logger;

  static {
    logger = Logger.getRootLogger();
  }

  public static void main(final String[] args) throws Exception {
    Properties jobProps = HadoopSecureWrapperUtils.loadAzkabanProps();
    props = new Props(null, jobProps);
    HadoopConfigurationInjector.injectResources(props);

    // special feature of secure pig wrapper: we will append the pig error file
    // onto system out
    pigLogFile = new File(System.getenv(PigCommonConstants.PIG_LOG_FILE));

    if (HadoopSecureWrapperUtils.shouldProxy(jobProps)) {
      String tokenFile = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
      UserGroupInformation proxyUser = HadoopSecureWrapperUtils.setupProxyUser(jobProps, tokenFile, logger);
      proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          runPigJob(args);
          return null;
        }
      });
    } else {
      runPigJob(args);
    }
  }

  @SuppressWarnings("deprecation")
  public static void runPigJob(String[] args) throws Exception {
    PigStats stats = PigUtil.runPigJob(args, props);

    PigUtil.dumpHadoopCounters(stats, props);

    if (stats.isSuccessful()) {
      return;
    }

    if (pigLogFile != null) {
      handleError(pigLogFile);
    }

    PigUtil.selfKill();
  }

  @SuppressWarnings("DefaultCharset")
  private static void handleError(File pigLog) throws Exception {
    System.out.println();
    System.out.println("Pig logfile dump:");
    System.out.println();
    try {
      BufferedReader reader = new BufferedReader(new FileReader(pigLog));
      String line = reader.readLine();
      while (line != null) {
        System.err.println(line);
        line = reader.readLine();
      }
      reader.close();
    } catch (FileNotFoundException e) {
      System.err.println("pig log file: " + pigLog + "  not found.");
    }
  }
}
