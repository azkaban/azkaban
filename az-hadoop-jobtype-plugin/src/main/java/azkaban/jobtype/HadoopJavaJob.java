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

import azkaban.utils.FileIOUtils;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.JavaProcessJob;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;


public class HadoopJavaJob extends AbstractHadoopJavaProcessJob {

  public static final String RUN_METHOD_PARAM = "method.run";
  public static final String CANCEL_METHOD_PARAM = "method.cancel";
  public static final String PROGRESS_METHOD_PARAM = "method.progress";

  public static final String JOB_CLASS = "job.class";
  public static final String DEFAULT_CANCEL_METHOD = "cancel";
  public static final String DEFAULT_RUN_METHOD = "run";
  public static final String DEFAULT_PROGRESS_METHOD = "getProgress";

  private String _runMethod;
  private String _cancelMethod;
  private String _progressMethod;

  private Object _javaObject = null;

  private boolean noUserClasspath = false;

  public HadoopJavaJob(String jobid, Props sysProps, Props jobProps, Logger log)
      throws RuntimeException {
    super(jobid, sysProps, jobProps, log);
    getJobProps().put(CommonJobProperties.JOB_ID, jobid);
    noUserClasspath = getSysProps().getBoolean("azkaban.no.user.classpath", false);
  }

  @Override
  protected String getJVMArguments() {
    String args = super.getJVMArguments();

    String typeUserGlobalJVMArgs =
        getJobProps().getString("jobtype.global.jvm.args", null);
    if (typeUserGlobalJVMArgs != null) {
      args += " " + typeUserGlobalJVMArgs;
    }
    String typeSysGlobalJVMArgs =
        getSysProps().getString("jobtype.global.jvm.args", null);
    if (typeSysGlobalJVMArgs != null) {
      args += " " + typeSysGlobalJVMArgs;
    }
    String typeUserJVMArgs = getJobProps().getString("jobtype.jvm.args", null);
    if (typeUserJVMArgs != null) {
      args += " " + typeUserJVMArgs;
    }
    String typeSysJVMArgs = getSysProps().getString("jobtype.jvm.args", null);
    if (typeSysJVMArgs != null) {
      args += " " + typeSysJVMArgs;
    }
    return args;
  }

  @Override
  protected List<String> getClassPaths() {
    List<String> classPath;
    if (!noUserClasspath) {
      classPath = super.getClassPaths();
    } else {
      getLog().info("Supressing user supplied classpath settings.");
      classPath = new ArrayList<String>();
    }

    classPath.add(FileIOUtils.getSourcePathFromClass(HadoopJavaJobRunnerMain.class));

    /**
     * Todo kunkun-tang: The legacy code uses a quite outdated method to resolve
     * Azkaban dependencies, and should be replaced later.
     */

    // To add az-core jar classpath
    classPath.add(FileIOUtils.getSourcePathFromClass(Props.class));

    // To add az-common jar classpath
    classPath.add(FileIOUtils.getSourcePathFromClass(JavaProcessJob.class));
    classPath.add(FileIOUtils.getSourcePathFromClass(HadoopSecurityManager.class));

    classPath.add(HadoopConfigurationInjector.getPath(getJobProps(),
        getWorkingDirectory()));

    // merging classpaths from plugin.properties
    List<String> jobTypeClassPath = getJobProps().getStringList("jobtype.classpath", null, ",");
    mergeTypeClassPaths(classPath, jobTypeClassPath);

    // merging classpaths from private.properties
    List<String> sysTypeClassPath = getSysProps().getStringList("jobtype.classpath", null, ",");
    mergeTypeClassPaths(classPath, sysTypeClassPath);

    List<String> typeGlobalClassPath =
        getSysProps().getStringList("jobtype.global.classpath", null, ",");
    mergeTypeGlobalClassPaths(classPath, typeGlobalClassPath);

    return classPath;
  }

  @Override
  public void run() throws Exception {
    String[] tagKeys = new String[] { CommonJobProperties.EXEC_ID,
        CommonJobProperties.FLOW_ID, CommonJobProperties.PROJECT_NAME };
    getJobProps().put(HadoopConfigurationInjector.INJECT_PREFIX
            + HadoopJobUtils.MAPREDUCE_JOB_TAGS,
        HadoopJobUtils.constructHadoopTags(getJobProps(), tagKeys));
    HadoopConfigurationInjector.prepareResourcesToInject(getJobProps(),
        getWorkingDirectory());

    super.run();
  }

  @Override
  protected String getJavaClass() {
    return HadoopJavaJobRunnerMain.class.getName();
  }

  @Override
  public String toString() {
    return "JavaJob{" + "_runMethod='" + _runMethod + '\''
        + ", _cancelMethod='" + _cancelMethod + '\'' + ", _progressMethod='"
        + _progressMethod + '\'' + ", _javaObject=" + _javaObject + ", props="
        + getJobProps() + '}';
  }

  /**
   * This cancel method, in addition to the default canceling behavior, also
   * kills the MR jobs launched by this job on Hadoop
   */
  @Override
  public void cancel() throws InterruptedException {
    super.cancel();

    info("Cancel called.  Killing the launched MR jobs on the cluster");

    final String logFilePath = jobProps.getString(CommonJobProperties.JOB_LOG_FILE);
    info("Log file path is: " + logFilePath);

    HadoopJobUtils.proxyUserKillAllSpawnedHadoopJobs(logFilePath, jobProps,
        tokenFile, getLog());
  }
}
