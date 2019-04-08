/*
 * Copyright 2016 LinkedIn Corp.
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

import java.io.File;
import azkaban.jobExecutor.ProcessJob;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import azkaban.flow.CommonJobProperties;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;


/**
 * HadoopShell is a Hadoop security enabled "command" jobtype. This jobtype
 * adheres to same format and other details as "command" jobtype.
 *
 * @author gaggarwa
 *
 */
public class HadoopShell extends ProcessJob {
  public static final String HADOOP_OPTS = ENV_PREFIX + "HADOOP_OPTS";
  public static final String HADOOP_GLOBAL_OPTS = "hadoop.global.opts";
  public static final String WHITELIST_REGEX = "command.whitelist.regex";
  public static final String BLACKLIST_REGEX = "command.blacklist.regex";

  private String userToProxy = null;
  private boolean shouldProxy = false;
  private boolean obtainTokens = false;
  private File tokenFile = null;
  private HadoopSecurityManager hadoopSecurityManager;

  public HadoopShell(String jobid, Props sysProps, Props jobProps, Logger log) throws RuntimeException {
    super(jobid, sysProps, jobProps, log);

    shouldProxy = getSysProps().getBoolean(HadoopSecurityManager.ENABLE_PROXYING, false);
    getJobProps().put(HadoopSecurityManager.ENABLE_PROXYING, Boolean.toString(shouldProxy));
    obtainTokens = getSysProps().getBoolean(HadoopSecurityManager.OBTAIN_BINARY_TOKEN, false);

    if (shouldProxy) {
      getLog().info("Initiating hadoop security manager.");
      try {
        hadoopSecurityManager = HadoopJobUtils.loadHadoopSecurityManager(getSysProps(), log);
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw new RuntimeException("Failed to get hadoop security manager!" + e.getCause());
      }
    }
  }


  //  @Override
  protected void setupPropsForProxy() throws Exception {
    if (shouldProxy && obtainTokens) {
      userToProxy = getJobProps().getString(HadoopSecurityManager.USER_TO_PROXY);
      getLog().info("Need to proxy. Getting tokens.");
      // get tokens in to a file, and put the location in props
      Props props = new Props();
      props.putAll(getJobProps());
      props.putAll(getSysProps());

      tokenFile = HadoopJobUtils.getHadoopTokens(hadoopSecurityManager, props, getLog());
      getJobProps().put("env." + HADOOP_TOKEN_FILE_LOCATION, tokenFile.getAbsolutePath());
    }
  }

  @Override
  public void run() throws Exception {
    setupHadoopOpts(getJobProps());
    HadoopConfigurationInjector.prepareResourcesToInject(getJobProps(), getWorkingDirectory());

    setupPropsForProxy();

    try {
      super.run();
    } catch (Throwable e) {
      e.printStackTrace();
      getLog().error("caught error running the job");
      throw new Exception(e);
    } finally {
      HadoopJobUtils.cancelHadoopTokens(hadoopSecurityManager, userToProxy, tokenFile, getLog());
    }
  }

  /**
   * Append HADOOP_GLOBAL_OPTS with HADOOP_OPTS in the given props
   *
   * @param props
   **/
  private void setupHadoopOpts(Props props) {
    if (props.containsKey(HADOOP_GLOBAL_OPTS)) {
      String hadoopGlobalOps = props.getString(HADOOP_GLOBAL_OPTS);
      if (props.containsKey(HADOOP_OPTS)) {
        String hadoopOps = props.getString(HADOOP_OPTS);
        props.put(HADOOP_OPTS, String.format("%s %s", hadoopOps, hadoopGlobalOps));
      } else {
        props.put(HADOOP_OPTS, hadoopGlobalOps);
      }
    }
  }

  @Override
  protected List<String> getCommandList() {
    // Use the same parsing login as in default "command job";
    List<String> commands = super.getCommandList();

    return HadoopJobUtils.filterCommands(
        commands,
        getSysProps().getString(WHITELIST_REGEX, HadoopJobUtils.MATCH_ALL_REGEX), // ".*" will match everything
        getSysProps().getString(BLACKLIST_REGEX, HadoopJobUtils.MATCH_NONE_REGEX),
        getLog()
    ); // ".^" will match nothing
  }

  /**
   * This cancel method, in addition to the default canceling behavior, also
   * kills the MR jobs launched by this job on Hadoop
   */
  @Override
  public void cancel() throws InterruptedException {
    super.cancel();

    info("Cancel called.  Killing the launched Hadoop jobs on the cluster");

    final String logFilePath = jobProps.getString(CommonJobProperties.JOB_LOG_FILE);
    info("Log file path is: " + logFilePath);

    HadoopJobUtils.proxyUserKillAllSpawnedHadoopJobs(logFilePath, jobProps, tokenFile, getLog());
  }
}
