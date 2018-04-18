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
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.pig.PigRunner;

import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.JavaProcessJob;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;

/*
 * need lib:
 * apache pig
 * hadoop-core*.jar
 * HadoopSecurePigWrapper
 * HadoopSecurityManager(corresponding version with hadoop)
 * abandon support for pig 0.8 and prior versions. don't see a use case here.
 */

public class HadoopPigJob extends JavaProcessJob {

  public static final String PIG_SCRIPT = "pig.script";
  public static final String UDF_IMPORT = "udf.import.list";
  public static final String PIG_ADDITIONAL_JARS = "pig.additional.jars";
  public static final String DEFAULT_PIG_ADDITIONAL_JARS =
      "default.pig.additional.jars";
  public static final String PIG_PARAM_PREFIX = "param.";
  public static final String PIG_PARAM_FILES = "paramfile";
  public static final String HADOOP_UGI = "hadoop.job.ugi";
  public static final String DEBUG = "debug";

  public static String HADOOP_SECURE_PIG_WRAPPER =
      "azkaban.jobtype.HadoopSecurePigWrapper";

  private String userToProxy = null;
  private boolean shouldProxy = false;
  private boolean obtainTokens = false;
  File tokenFile = null;

  private final boolean userPigJar;

  private HadoopSecurityManager hadoopSecurityManager;

  private File pigLogFile = null;

  public HadoopPigJob(String jobid, Props sysProps, Props jobProps, Logger log)
      throws IOException {
    super(jobid, sysProps, jobProps, log);

    HADOOP_SECURE_PIG_WRAPPER = HadoopSecurePigWrapper.class.getName();

    getJobProps().put(CommonJobProperties.JOB_ID, jobid);
    shouldProxy =
        getSysProps().getBoolean(HadoopSecurityManager.ENABLE_PROXYING, false);
    getJobProps().put(HadoopSecurityManager.ENABLE_PROXYING,
        Boolean.toString(shouldProxy));
    obtainTokens =
        getSysProps().getBoolean(HadoopSecurityManager.OBTAIN_BINARY_TOKEN,
            false);
    userPigJar = getJobProps().getBoolean("use.user.pig.jar", false);

    if (shouldProxy) {
      getLog().info("Initiating hadoop security manager.");
      try {
        hadoopSecurityManager =
            HadoopJobUtils.loadHadoopSecurityManager(getSysProps(), log);
      } catch (RuntimeException e) {
        throw new RuntimeException("Failed to get hadoop security manager!" + e);
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
      // get tokens in to a file, and put the location in props
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
    } catch (Throwable t) {
      t.printStackTrace();
      getLog().error("caught error running the job", t);
      throw new Exception(t);
    } finally {
      if (tokenFile != null) {
        HadoopJobUtils.cancelHadoopTokens(hadoopSecurityManager, userToProxy,
            tokenFile, getLog());
        if (tokenFile.exists()) {
          tokenFile.delete();
        }
      }
    }
  }

  @Override
  protected String getJavaClass() {
    return HADOOP_SECURE_PIG_WRAPPER;
  }

  @Override
  protected String getJVMArguments() {
    String args = super.getJVMArguments();

    String typeGlobalJVMArgs =
        getSysProps().getString("jobtype.global.jvm.args", null);
    if (typeGlobalJVMArgs != null) {
      args += " " + typeGlobalJVMArgs;
    }

    List<String> udfImport = getUDFImportList();
    if (udfImport.size() > 0) {
      args += " -Dudf.import.list=" + super.createArguments(udfImport, ":");
    }

    List<String> additionalJars = getAdditionalJarsList();
    if (additionalJars.size() > 0) {
      args +=
          " -Dpig.additional.jars="
              + super.createArguments(additionalJars, ":");
    }

    String hadoopUGI = getHadoopUGI();
    if (hadoopUGI != null) {
      args += " -Dhadoop.job.ugi=" + hadoopUGI;
    }

    if (shouldProxy) {
      info("Setting up secure proxy info for child process");
      String secure;
      secure =
          " -D" + HadoopSecurityManager.USER_TO_PROXY + "="
              + getJobProps().getString(HadoopSecurityManager.USER_TO_PROXY);
      String extraToken =
          getSysProps().getString(HadoopSecurityManager.OBTAIN_BINARY_TOKEN,
              "false");
      if (extraToken != null) {
        secure +=
            " -D" + HadoopSecurityManager.OBTAIN_BINARY_TOKEN + "="
                + extraToken;
      }
      info("Secure settings = " + secure);
      args += secure;
    } else {
      info("Not setting up secure proxy info for child process");
    }

    return args;
  }

  @Override
  protected String getMainArguments() {
    ArrayList<String> list = new ArrayList<String>();
    Map<String, String> map = getPigParams();
    if (map != null) {
      for (Map.Entry<String, String> entry : map.entrySet()) {
        list.add("-param "
            + StringUtils.shellQuote(entry.getKey() + "=" + entry.getValue(),
                StringUtils.SINGLE_QUOTE));
      }
    }

    List<String> paramFiles = getPigParamFiles();
    if (paramFiles != null) {
      for (String paramFile : paramFiles) {
        list.add("-param_file " + paramFile);
      }
    }

    if (getDebug()) {
      list.add("-debug");
    }

    try {
      pigLogFile =
          File.createTempFile("piglogfile", ".log", new File(
              getWorkingDirectory()));
      jobProps.put("env." + "PIG_LOG_FILE", pigLogFile.getAbsolutePath());
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (pigLogFile != null) {
      list.add("-logfile " + pigLogFile.getAbsolutePath());
    }

    list.add(getScript());

    return StringUtils.join((Collection<String>) list, " ");
  }

  @Override
  protected List<String> getClassPaths() {

    List<String> classPath = super.getClassPaths();

    // To add az-core jar classpath
    classPath.add(getSourcePathFromClass(Props.class));

    // To add az-common jar classpath
    classPath.add(getSourcePathFromClass(JavaProcessJob.class));
    classPath.add(getSourcePathFromClass(HadoopSecurePigWrapper.class));
    classPath.add(getSourcePathFromClass(HadoopSecurityManager.class));

    classPath.add(HadoopConfigurationInjector.getPath(getJobProps(),
        getWorkingDirectory()));

    // assuming pig 0.8 and up
    if (!userPigJar) {
      classPath.add(getSourcePathFromClass(PigRunner.class));
    }

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

  protected boolean getDebug() {
    return getJobProps().getBoolean(DEBUG, false);
  }

  protected String getScript() {
    return getJobProps().getString(PIG_SCRIPT);
  }

  protected List<String> getUDFImportList() {
    List<String> udfImports = new ArrayList<String>();
    List<String> typeImports =
        getSysProps().getStringList(UDF_IMPORT, null, ",");
    List<String> jobImports =
        getJobProps().getStringList(UDF_IMPORT, null, ",");
    if (typeImports != null) {
      udfImports.addAll(typeImports);
    }
    if (jobImports != null) {
      udfImports.addAll(jobImports);
    }
    return udfImports;
  }

  protected List<String> getAdditionalJarsList() {
    List<String> additionalJars = new ArrayList<String>();
    mergeAdditionalJars(additionalJars, PIG_ADDITIONAL_JARS);
    mergeAdditionalJars(additionalJars, DEFAULT_PIG_ADDITIONAL_JARS);
    return additionalJars;
  }

  /**
   * Merging all additional jars first from user specified/plugin.properties
   * then private.properties for additionalJarProperty property
   */
  private void mergeAdditionalJars(List<String> additionalJars,
      String additionalJarProperty) {
    List<String> jobJars =
        getJobProps().getStringList(additionalJarProperty, null, ",");
    List<String> typeJars =
        getSysProps().getStringList(additionalJarProperty, null, ",");
    if (jobJars != null) {
      additionalJars.addAll(jobJars);
    }
    if (typeJars != null) {
      additionalJars.addAll(typeJars);
    }
  }

  protected String getHadoopUGI() {
    return getJobProps().getString(HADOOP_UGI, null);
  }

  protected Map<String, String> getPigParams() {
    return getJobProps().getMapByPrefix(PIG_PARAM_PREFIX);
  }

  protected List<String> getPigParamFiles() {
    return getJobProps().getStringList(PIG_PARAM_FILES, null, ",");
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

  /**
   * This cancel method, in addition to the default canceling behavior, also
   * kills the MR jobs launched by Pig on Hadoop
   */
  @Override
  public void cancel() throws InterruptedException {
    super.cancel();

    info("Cancel called.  Killing the Pig launched MR jobs on the cluster");

    String azExecId = jobProps.getString(CommonJobProperties.EXEC_ID);
    final String logFilePath =
        String.format("%s/_job.%s.%s.log", getWorkingDirectory(), azExecId,
            getId());
    info("log file path is: " + logFilePath);

    HadoopJobUtils.proxyUserKillAllSpawnedHadoopJobs(logFilePath, jobProps,
        tokenFile, getLog());
  }
}
