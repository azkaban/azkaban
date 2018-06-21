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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;

import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.JavaProcessJob;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;

public class HadoopJavaJob extends JavaProcessJob {

  public static final String RUN_METHOD_PARAM = "method.run";
  public static final String CANCEL_METHOD_PARAM = "method.cancel";
  public static final String PROGRESS_METHOD_PARAM = "method.progress";

  public static final String JOB_CLASS = "job.class";
  public static final String DEFAULT_CANCEL_METHOD = "cancel";
  public static final String DEFAULT_RUN_METHOD = "run";
  public static final String DEFAULT_PROGRESS_METHOD = "getProgress";

  private static final Pattern AZKABAN_EXEC_ID_PATTERN = Pattern.compile("-Dazkaban.execid=(\\d+)");

  private String _runMethod;
  private String _cancelMethod;
  private String _progressMethod;

  private Object _javaObject = null;

  private String userToProxy = null;
  private boolean shouldProxy = false;
  private boolean obtainTokens = false;
  private boolean noUserClasspath = false;
  private File tokenFile = null;

  private HadoopSecurityManager hadoopSecurityManager;

  public HadoopJavaJob(String jobid, Props sysProps, Props jobProps, Logger log)
      throws RuntimeException {
    super(jobid, sysProps, jobProps, log);

    getJobProps().put(CommonJobProperties.JOB_ID, jobid);
    shouldProxy =
        getSysProps().getBoolean(HadoopSecurityManager.ENABLE_PROXYING, false);
    getJobProps().put(HadoopSecurityManager.ENABLE_PROXYING,
        Boolean.toString(shouldProxy));
    obtainTokens =
        getSysProps().getBoolean(HadoopSecurityManager.OBTAIN_BINARY_TOKEN,
            false);
    noUserClasspath =
        getSysProps().getBoolean("azkaban.no.user.classpath", false);

    if (shouldProxy) {
      getLog().info("Initiating hadoop security manager.");
      try {
        hadoopSecurityManager =
            HadoopJobUtils.loadHadoopSecurityManager(getSysProps(), log);
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw new RuntimeException("Failed to get hadoop security manager!"
            + e.getCause());
      }
    }
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

    classPath.add(getSourcePathFromClass(HadoopJavaJobRunnerMain.class));

    /**
     * Todo kunkun-tang: The legacy code uses a quite outdated method to resolve
     * Azkaban dependencies, and should be replaced later.
     */

    // To add az-core jar classpath
    classPath.add(getSourcePathFromClass(Props.class));

    // To add az-common jar classpath
    classPath.add(getSourcePathFromClass(JavaProcessJob.class));
    classPath.add(getSourcePathFromClass(HadoopSecurityManager.class));

    classPath.add(HadoopConfigurationInjector.getPath(getJobProps(),
        getWorkingDirectory()));

    // merging classpaths from plugin.properties
    mergeClassPaths(classPath,
        getJobProps().getStringList("jobtype.classpath", null, ","));
    // merging classpaths from private.properties
    mergeClassPaths(classPath,
        getSysProps().getStringList("jobtype.classpath", null, ","));

    List<String> typeGlobalClassPath =
        getSysProps().getStringList("jobtype.global.classpath", null, ",");
    if (typeGlobalClassPath != null) {
      for (String jar : typeGlobalClassPath) {
        if (!classPath.contains(jar)) {
          classPath.add(jar);
        }
      }
    }

    return classPath;
  }

  private void mergeClassPaths(List<String> classPath,
      List<String> typeClassPath) {
    if (typeClassPath != null) {
      // fill in this when load this jobtype
      String pluginDir = getSysProps().get("plugin.dir");
      for (String jar : typeClassPath) {
        File jarFile = new File(jar);
        if (!jarFile.isAbsolute()) {
          jarFile = new File(pluginDir + File.separatorChar + jar);
        }

        if (!classPath.contains(jarFile.getAbsolutePath())) {
          classPath.add(jarFile.getAbsolutePath());
        }
      }
    }
  }

  @Override
  public void run() throws Exception {
    HadoopConfigurationInjector.prepareResourcesToInject(getJobProps(),
        getWorkingDirectory());

    if (shouldProxy && obtainTokens) {
      userToProxy = getJobProps().getString("user.to.proxy");
      getLog().info("Need to proxy. Getting tokens.");
      Props props = new Props();
      props.putAll(getJobProps());
      props.putAll(getSysProps());

      HadoopJobUtils.addAdditionalNamenodesToPropsFromMRJob(props, getLog());
      tokenFile =
          HadoopJobUtils
              .getHadoopTokens(hadoopSecurityManager, props, getLog());
      getJobProps().put("env." + HADOOP_TOKEN_FILE_LOCATION,
          tokenFile.getAbsolutePath());
    }
    try {
      super.run();
    } catch (Exception e) {
      e.printStackTrace();
      throw new Exception(e);
    } finally {
      if (tokenFile != null) {
        try {
          HadoopJobUtils.cancelHadoopTokens(hadoopSecurityManager, userToProxy,
              tokenFile, getLog());
        } catch (Throwable t) {
          t.printStackTrace();
          getLog().error("Failed to cancel tokens.");
        }
        if (tokenFile.exists()) {
          tokenFile.delete();
        }
      }
    }
  }

  private static String getSourcePathFromClass(Class<?> containedClass) {
    File file =
        new File(containedClass.getProtectionDomain().getCodeSource()
            .getLocation().getPath());

    if (!file.isDirectory() && file.getName().endsWith(".class")) {
      String name = containedClass.getName();
      StringTokenizer tokenizer = new StringTokenizer(name, ".");
      while (tokenizer.hasMoreTokens()) {
        tokenizer.nextElement();

        file = file.getParentFile();
      }
      return file.getPath();
    } else {
      return containedClass.getProtectionDomain().getCodeSource().getLocation()
          .getPath();
    }
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
   * Returns the {@code azkaban.execid} system property, if set, and otherwise returns the value of
   * {@value CommonJobProperties#EXEC_ID}. {@value CommonJobProperties#EXEC_ID} may be overridden by a flow param or
   * job property, whereas the {@code azkaban.execid} system property will be the unique execution id Azkaban generates
   * for each flow.
   */
  private String getExecId() {
    String jvmArgs = getJVMArguments();
    Matcher matcher = AZKABAN_EXEC_ID_PATTERN.matcher(jvmArgs);
    if (matcher.find()) {
      return matcher.group(1);
    }

    return jobProps.getString(CommonJobProperties.EXEC_ID);
  }

  /**
   * This cancel method, in addition to the default canceling behavior, also
   * kills the MR jobs launched by this job on Hadoop
   */
  @Override
  public void cancel() throws InterruptedException {
    super.cancel();

    info("Cancel called.  Killing the launched MR jobs on the cluster");

    String azExecId = getExecId();
    final String logFilePath =
        String.format("%s/_job.%s.%s.log", getWorkingDirectory(), azExecId,
            getId());
    info("log file path is: " + logFilePath);

    HadoopJobUtils.proxyUserKillAllSpawnedHadoopJobs(logFilePath, jobProps,
        tokenFile, getLog());
  }
}
