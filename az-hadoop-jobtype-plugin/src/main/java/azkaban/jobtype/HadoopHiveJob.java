/*
 * Copyright 2014 LinkedIn Corp.
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

import azkaban.flow.CommonJobProperties;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 * Hadoop Hive Job Plugin
 */
public class HadoopHiveJob extends AbstractHadoopJavaProcessJob {

  private static final String HIVE_SCRIPT = "hive.script";
  private static final String HIVECONF_PARAM_PREFIX = "hiveconf.";
  private static final String HIVEVAR_PARAM_PREFIX = "hivevar.";
  private static final String HADOOP_SECURE_HIVE_WRAPPER =
      "azkaban.jobtype.HadoopSecureHiveWrapper";

  private boolean debug = false;

  public HadoopHiveJob(String jobid, Props sysProps, Props jobProps, Logger log) {
    super(jobid, sysProps, jobProps, log);
    getJobProps().put(CommonJobProperties.JOB_ID, jobid);
    debug = getJobProps().getBoolean("debug", false);
  }

  @Override
  public void run() throws Exception {
    setupHadoopJobProperties();
    HadoopConfigurationInjector.prepareResourcesToInject(getJobProps(), getWorkingDirectory());
    getHadoopProxy().setupPropsForProxy(getAllProps(), getJobProps(), getLog());
    super.run();
  }

  @Override
  protected String getJavaClass() {
    return HADOOP_SECURE_HIVE_WRAPPER;
  }

  @Override
  protected String getJVMArguments() {
    String args = super.getJVMArguments();
    args += getJVMJobTypeParameters();
    args += getJVMProxySecureArgument();
    return args;
  }

  @Override
  protected String getMainArguments() {
    ArrayList<String> list = new ArrayList<String>();

    // for hiveconf
    Map<String, String> map = getHiveConf();
    if (map != null) {
      for (Map.Entry<String, String> entry : map.entrySet()) {
        list.add("-hiveconf");
        list.add(StringUtils.shellQuote(
            entry.getKey() + "=" + entry.getValue(), StringUtils.SINGLE_QUOTE));
      }
    }

    if (debug) {
      list.add("-hiveconf");
      list.add("hive.root.logger=INFO,console");
    }

    // for hivevar
    Map<String, String> hiveVarMap = getHiveVar();
    if (hiveVarMap != null) {
      for (Map.Entry<String, String> entry : hiveVarMap.entrySet()) {
        list.add("-hivevar");
        list.add(StringUtils.shellQuote(
            entry.getKey() + "=" + entry.getValue(), StringUtils.SINGLE_QUOTE));
      }
    }

    list.add("-f");
    list.add(getScript());

    return StringUtils.join((Collection<String>) list, " ");
  }

  @Override
  protected List<String> getClassPaths() {

    List<String> classPath = super.getClassPaths();

    classPath.addAll(getAzkabanCommonClassPaths());

    classPath.add(HadoopConfigurationInjector.getPath(getJobProps(),
        getWorkingDirectory()));

    return mergeSysTypeClassPaths(classPath);
  }

  private String getScript() {
    return getJobProps().getString(HIVE_SCRIPT);
  }

  private Map<String, String> getHiveConf() {
    return getJobProps().getMapByPrefix(HIVECONF_PARAM_PREFIX);
  }

  private Map<String, String> getHiveVar() {
    return getJobProps().getMapByPrefix(HIVEVAR_PARAM_PREFIX);
  }

  /**
   * This cancel method, in addition to the default canceling behavior, also kills the MR jobs
   * launched by Hive
   * on Hadoop
   */
  @Override
  public void cancel() throws InterruptedException {
    super.cancel();

    info("Cancel called.  Killing the Hive launched MR jobs on the cluster");

    getHadoopProxy().killAllSpawnedHadoopJobs(getJobProps(), getLog());
  }
}
