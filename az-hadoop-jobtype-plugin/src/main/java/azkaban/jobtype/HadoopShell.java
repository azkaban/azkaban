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

import java.util.List;
import org.apache.log4j.Logger;
import azkaban.jobExecutor.ProcessJob;
import azkaban.utils.Props;


/**
 * HadoopShell is a Hadoop security enabled "command" jobtype. This jobtype
 * adheres to same format and other details as "command" jobtype.
 *
 * @author gaggarwa
 */
public class HadoopShell extends ProcessJob implements IHadoopJob {

  public static final String HADOOP_OPTS = ENV_PREFIX + "HADOOP_OPTS";
  public static final String HADOOP_GLOBAL_OPTS = "hadoop.global.opts";
  public static final String WHITELIST_REGEX = "command.whitelist.regex";
  public static final String BLACKLIST_REGEX = "command.blacklist.regex";

  private final HadoopProxy hadoopProxy;

  public HadoopShell(String jobid, Props sysProps, Props jobProps, Logger logger)
      throws RuntimeException {
    super(jobid, sysProps, jobProps, logger);
    this.hadoopProxy = new HadoopProxy(sysProps, jobProps, logger);
  }

  @Override
  public void run() throws Exception {
    setupHadoopJobProperties();
    HadoopConfigurationInjector.prepareResourcesToInject(getJobProps(), getWorkingDirectory());
    hadoopProxy.setupPropsForProxy(getAllProps(), getJobProps(), getLog());

    try {
      super.run();
    } catch (Throwable e) {
      e.printStackTrace();
      getLog().error("caught error running the job");
      throw e;
    //} finally {
      //hadoopProxy.cancelHadoopTokens(getLog());
    }
  }

  @Override
  public HadoopProxy getHadoopProxy() {
    return this.hadoopProxy;
  }

  /**
   * Append HADOOP_GLOBAL_OPTS with HADOOP_OPTS in the given props
   **/
  @Override
  public void setupHadoopJobProperties() {
    if (getJobProps().containsKey(HADOOP_GLOBAL_OPTS)) {
      String hadoopGlobalOps = getJobProps().getString(HADOOP_GLOBAL_OPTS);
      if (getJobProps().containsKey(HADOOP_OPTS)) {
        String hadoopOps = getJobProps().getString(HADOOP_OPTS);
        getJobProps().put(HADOOP_OPTS, String.format("%s %s", hadoopOps, hadoopGlobalOps));
      } else {
        getJobProps().put(HADOOP_OPTS, hadoopGlobalOps);
      }
    }
  }

  @Override
  protected List<String> getCommandList() {
    // Use the same parsing login as in default "command job";
    List<String> commands = super.getCommandList();

    return HadoopJobUtils.filterCommands(
        commands,
        // ".*" will match everything
        getSysProps().getString(WHITELIST_REGEX, HadoopJobUtils.MATCH_ALL_REGEX),
        // ".^" will match nothing
        getSysProps().getString(BLACKLIST_REGEX, HadoopJobUtils.MATCH_NONE_REGEX),
        getLog()
    );
  }

  /**
   * This cancel method, in addition to the default canceling behavior, also
   * kills the MR jobs launched by this job on Hadoop
   */
  @Override
  public void cancel() throws InterruptedException {
    super.cancel();
    info("Cancel called.  Killing the launched Hadoop jobs on the cluster");
    hadoopProxy.killAllSpawnedHadoopJobs(getJobProps(), getLog());
  }
}
