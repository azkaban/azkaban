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

package azkaban.jobtype;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.pig.tools.pigstats.PigStats;

import azkaban.jobtype.pig.PigCommonConstants;
import azkaban.jobtype.pig.PigUtil;
import azkaban.jobtype.tuning.TuningCommonConstants;
import azkaban.jobtype.tuning.TuningErrorDetector;
import azkaban.jobtype.tuning.TuningException;
import azkaban.jobtype.tuning.TuningParameterUtils;
import azkaban.utils.Props;
import azkaban.jobExecutor.AbstractProcessJob;


/**
 * This class represent wrapper for running pig job with tuning enabled.
 */
public class HadoopTuningSecurePigWrapper {

  private static File pigLogFile;

  private static Props props;

  private static final Logger logger = Logger.getRootLogger();;

  /**
   * This function runs the pig job with tuning enabled. In case tuning job fails in first try, it asks for
   * last best parameter from tuning and re-run the job. In case tuning end point is not responding this job
   * will run with default parameters.
   *
   * @param args
   * @throws Throwable
   */
  public static void main(final String[] args) throws Exception {
    int jobTryCount = 1;
    Properties jobProps = HadoopSecureWrapperUtils.loadAzkabanProps();
    Props initialJobprops = new Props(null, jobProps);
    boolean retry = false;
    boolean firstTry = true;
    int maxJobRetry = 2;
    if (initialJobprops.containsKey(TuningCommonConstants.TUNING_JOB_RETRY_COUNT)) {
      maxJobRetry = initialJobprops.getInt(TuningCommonConstants.TUNING_JOB_RETRY_COUNT);
    }

    while (jobTryCount <= maxJobRetry && (retry || firstTry)) {
      TuningParameterUtils tuningParameterUtils = new TuningParameterUtils();
      jobTryCount++;
      firstTry = false;
      props = Props.clone(initialJobprops);
      props.put(TuningCommonConstants.AUTO_TUNING_RETRY, String.valueOf(retry));

      tuningParameterUtils.updateAutoTuningParameters(props);

      HadoopTuningConfigurationInjector.prepareResourcesToInject(props,
          HadoopTuningSecurePigWrapper.getWorkingDirectory(props));

      HadoopTuningConfigurationInjector.injectResources(props);

      // special feature of secure pig wrapper: we will append the pig error file
      // onto system out
      pigLogFile = new File(System.getenv(PigCommonConstants.PIG_LOG_FILE));

      try {
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
      } catch (TuningException t) {
        retry = false;
        System.out.println("TryCount:" + jobTryCount + ", maxRetry:" + maxJobRetry);
        if (jobTryCount <= maxJobRetry) {
          System.out.println("Error due to auto tuning parameters ");
          retry = true;
        } else {
          throw t;
        }
      } catch (Exception t) {
        throw t;
      }
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

  private static void handleError(File pigLog) throws Exception {
    System.out.println();
    System.out.println("Pig logfile dump:");
    System.out.println();
    try (BufferedReader reader = Files.newBufferedReader(pigLog.toPath(), Charset.defaultCharset())) {
      String line = reader.readLine();
      boolean isTuningError = false;
      TuningErrorDetector tuningErrorDetector = new TuningErrorDetector();
      while (line != null) {
        System.err.println(line);
        line = reader.readLine();
        //Checks if log have any predefined error pattern which can be caused by auto tuning parameters
        if (tuningErrorDetector.containsAutoTuningError(line)) {
          isTuningError = true;
        }
      }
      if (isTuningError) {
        throw new TuningException("Auto Tuning Failure");
      }
    } catch (FileNotFoundException e) {
      System.err.println("pig log file: " + pigLog + "  not found.");
    }
  }

  public static String getWorkingDirectory(Props props) {
    final String workingDir = props.getString(AbstractProcessJob.WORKING_DIR);
    if (workingDir == null) {
      return "";
    }

    return workingDir;
  }
}
