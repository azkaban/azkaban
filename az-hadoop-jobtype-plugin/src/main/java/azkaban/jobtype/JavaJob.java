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
package azkaban.jobtype;

import azkaban.utils.FileIOUtils;
import azkaban.utils.Utils;
import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;

import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.JavaProcessJob;
import azkaban.security.commons.SecurityUtils;
import azkaban.utils.Props;

public class JavaJob extends JavaProcessJob {

  private static final String RUN_METHOD_PARAM = "method.run";
  private static final String CANCEL_METHOD_PARAM = "method.cancel";
  private static final String PROGRESS_METHOD_PARAM = "method.progress";

  private static final String JOB_CLASS = "job.class";
  private static final String DEFAULT_CANCEL_METHOD = "cancel";
  private static final String DEFAULT_RUN_METHOD = "run";
  private static final String DEFAULT_PROGRESS_METHOD = "getProgress";

  private String _runMethod;
  private String _cancelMethod;
  private String _progressMethod;

  private Object _javaObject = null;

  public JavaJob(String jobid, Props sysProps, Props jobProps, Logger log) {
    super(jobid, sysProps, new Props(sysProps, jobProps), log);
    getJobProps().put(CommonJobProperties.JOB_ID, jobid);
  }

  @Override
  protected String getJVMArguments() {
    String args = super.getJVMArguments();

    String typeGlobalJVMArgs =
        getSysProps().getString("jobtype.global.jvm.args", null);
    if (typeGlobalJVMArgs != null) {
      args += " " + typeGlobalJVMArgs;
    }
    return args;
  }

  @Override
  @SuppressWarnings("CollectionIncompatibleType")
  protected List<String> getClassPaths() {
    List<String> classPath = super.getClassPaths();

    classPath.add(FileIOUtils.getSourcePathFromClass(JavaJobRunnerMain.class));
    // To add az-core jar classpath
    classPath.add(FileIOUtils.getSourcePathFromClass(Props.class));

    // To add az-common jar classpath
    classPath.add(FileIOUtils.getSourcePathFromClass(JavaProcessJob.class));
    classPath.add(FileIOUtils.getSourcePathFromClass(SecurityUtils.class));

    classPath.add(HadoopConfigurationInjector.getPath(getJobProps(),
        getWorkingDirectory()));

    String loggerPath = FileIOUtils.getSourcePathFromClass(Logger.class);
    if (!classPath.contains(loggerPath)) {
      classPath.add(loggerPath);
    }

    // Add hadoop home to classpath
    String hadoopHome = System.getenv("HADOOP_HOME");
    if (hadoopHome == null) {
      info("HADOOP_HOME not set, using default hadoop config.");
    } else {
      info("Using hadoop config found in " + hadoopHome);
      classPath.add(new File(hadoopHome, "conf").getPath());
    }

    List<String> typeClassPath =
        getSysProps().getStringList("jobtype.classpath", null, ",");
    Utils.mergeTypeClassPaths(classPath, typeClassPath, getSysProps().get("plugin.dir"));

    List<String> typeGlobalClassPath =
        getSysProps().getStringList("jobtype.global.classpath", null, ",");
    Utils.mergeStringList(classPath, typeGlobalClassPath);

    return classPath;
  }

  @Override
  protected String getJavaClass() {
    return JavaJobRunnerMain.class.getName();
  }

  @Override
  public String toString() {
    return "JavaJob{" + "_runMethod='" + _runMethod + '\''
        + ", _cancelMethod='" + _cancelMethod + '\'' + ", _progressMethod='"
        + _progressMethod + '\'' + ", _javaObject=" + _javaObject + ", props="
        + getAllProps().toString() + '}';
  }


  @Override
  public void run() throws Exception {
    HadoopConfigurationInjector.prepareResourcesToInject(
        getJobProps(), getWorkingDirectory()
    );
    super.run();
  }
}
