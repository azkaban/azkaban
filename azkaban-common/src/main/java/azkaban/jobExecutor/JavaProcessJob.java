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
package azkaban.jobExecutor;

import azkaban.flow.CommonJobProperties;
import azkaban.server.AzkabanServer;
import azkaban.utils.MemConfValue;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

public class JavaProcessJob extends ProcessJob {

  public static final String CLASSPATH = "classpath";
  public static final String GLOBAL_CLASSPATH = "global.classpaths";
  public static final String JAVA_CLASS = "java.class";
  public static final String INITIAL_MEMORY_SIZE = "Xms";
  public static final String MAX_MEMORY_SIZE = "Xmx";
  public static final String MAIN_ARGS = "main.args";
  public static final String JVM_PARAMS = "jvm.args";
  public static final String GLOBAL_JVM_PARAMS = "global.jvm.args";
  public static final String DEPENDENCY_CLS_RAMP_PROP_PREFIX = "azkaban.ramp.jar:";
  public static final String DEPENDENCY_REG_RAMP_PROP_PREFIX = "azkaban.ramp.reg:";
  public static final String DEPENDENCY_CFG_RAMP_PROP_PREFIX = "azkaban.ramp.cfg:";

  public static final String DEFAULT_INITIAL_MEMORY_SIZE = "64M";
  public static final String DEFAULT_MAX_MEMORY_SIZE = "256M";
  public static final String LIBRARY_PATH_PREFIX = "library.path.";
  public static final String NATIVE_LIBRARY_PATH_PREFIX = "native.library.path.";

  public static String JAVA_COMMAND = "java";

  public JavaProcessJob(final String jobid, final Props sysProps, final Props jobProps,
      final Logger logger) {
    super(jobid, sysProps, jobProps, logger);
  }

  public JavaProcessJob(final String jobid, final Props sysProps, final Props jobProps,
      final Props privateProps, final Logger logger) {
    super(jobid, sysProps, jobProps, privateProps, logger);
  }

  @Override
  protected List<String> getCommandList() {
    final ArrayList<String> list = new ArrayList<>();
    list.add(createCommandLine());
    return list;
  }

  protected String createCommandLine() {
    String command = JAVA_COMMAND + " ";
    command += getJVMArguments() + " ";
    command += "-Xms" + getInitialMemorySize() + " ";
    command += "-Xmx" + getMaxMemorySize() + " ";
    command += getClassPathParam();
    command += getJavaClass() + " ";
    command += getMainArguments();

    return command;
  }

  protected String getJavaClass() {
    return getJobProps().getString(JAVA_CLASS);
  }

  protected String getClassPathParam() {
    final List<String> classPath = getClassPaths();

    final List<String> clusterComponentClassPaths = getClusterComponentClassPath();
    getLog().info("Adding classpath of cluster components: " +
        String.join(":", clusterComponentClassPaths));
    classPath.addAll(clusterComponentClassPaths);
    if (classPath == null || classPath.size() == 0) {
      throw new IllegalArgumentException(
          "No classpath defined and no .jar files found in job directory. Can't run java command.");
    }

    return "-cp " + createArguments(classPath, ":") + " ";
  }

  private List<String> getClusterComponentClassPath() {
    return this.jobProps.getStringList(CommonJobProperties.TARGET_CLUSTER_CLASSPATH, ":");
  }

  protected List<String> getClassPaths() {

    final List<String> classPaths = getJobProps().getStringList(CLASSPATH, null, ",");

    final ArrayList<String> classpathList = new ArrayList<>();
    // Adding global properties used system wide.
    if (getJobProps().containsKey(GLOBAL_CLASSPATH)) {
      final List<String> globalClasspath =
          getJobProps().getStringList(GLOBAL_CLASSPATH);
      for (final String global : globalClasspath) {
        getLog().info("Adding to global classpath:" + global);
        classpathList.add(global);
      }
    }

    if (classPaths == null || classPaths.isEmpty()) {
      final File path = new File(getPath());
      getLog().info(
          "No classpath specified. Trying to load classes from " + path);

      if (path != null) {
        for (final File file : path.listFiles()) {
          if (file.getName().endsWith(".jar")) {
            classpathList.add(file.getName());
          }
        }
      }
    } else {
      classpathList.addAll(classPaths);
    }

    return classpathList;
  }

  protected Map<String, String> getRampItems(String prefix) {
    Map<String, String> rampItems = getJobProps()
        .getKeySet()
        .stream()
        .filter(propKey -> propKey.startsWith(prefix))
        .collect(Collectors.toMap(
            key -> key,
            key -> getJobProps().get(key)
        ));
    if (!rampItems.isEmpty()) {
      getLog().info(String.format("[Ramp Items] : %s", rampItems.toString()));
    }
    return rampItems;
  }

  protected List<String> mergeSysTypeClassPaths(List<String> classPath) {
    Utils.mergeTypeClassPaths(classPath,
        getSysProps().getStringList("jobtype.classpath", null, ","),
        getSysProps().get("plugin.dir"));

    Utils.mergeStringList(classPath,
        getSysProps().getStringList("jobtype.global.classpath", null, ","));

    return classPath;
  }

  protected String getInitialMemorySize() {
    return getJobProps().getString(INITIAL_MEMORY_SIZE,
        DEFAULT_INITIAL_MEMORY_SIZE);
  }

  protected String getMaxMemorySize() {
    return getJobProps().getString(MAX_MEMORY_SIZE, DEFAULT_MAX_MEMORY_SIZE);
  }

  protected String getMainArguments() {
    return getJobProps().getString(MAIN_ARGS, "");
  }

  protected String getJVMArguments() {
    final String globalJVMArgs = getJobProps().getString(GLOBAL_JVM_PARAMS, null);

    final String nativeLibPath = getNativeLibraryAsJVMArguments();

    if (globalJVMArgs == null) {
      return String.format("%s %s", nativeLibPath, getJobProps().getString(JVM_PARAMS, ""));
    }

    return String.format("%s %s %s", globalJVMArgs, nativeLibPath, getJobProps().getString(JVM_PARAMS, ""));
  }

  private String getNativeLibraryAsJVMArguments() {
    return String.format("-Djava.library.path=%s",
        this.jobProps.get(CommonJobProperties.TARGET_CLUSTER_NATIVE_LIB));
  }

  protected String createArguments(final List<String> arguments, final String separator) {
    if (arguments != null && arguments.size() > 0) {
      String param = "";
      for (final String arg : arguments) {
        param += arg + separator;
      }

      return param.substring(0, param.length() - 1);
    }

    return "";
  }

  @Override
  protected Pair<Long, Long> getProcMemoryRequirement() throws Exception {
    final String strXms = getInitialMemorySize();
    final String strXmx = getMaxMemorySize();
    final long xms = Utils.parseMemString(strXms);
    final long xmx = Utils.parseMemString(strXmx);

    final Props azkabanProperties = AzkabanServer.getAzkabanProperties();
    if (azkabanProperties != null) {
      final MemConfValue maxXms = MemConfValue.parseMaxXms(azkabanProperties);
      final MemConfValue maxXmx = MemConfValue.parseMaxXmx(azkabanProperties);

      if (xms > maxXms.getSize()) {
        throw new Exception(
            String.format("%s: Xms value has exceeded the allowed limit (max Xms = %s)",
                getId(), maxXms.getString()));
      }

      if (xmx > maxXmx.getSize()) {
        throw new Exception(
            String.format("%s: Xmx value has exceeded the allowed limit (max Xmx = %s)",
                getId(), maxXmx.getString()));
      }
    }

    return new Pair<>(xms, xmx);
  }
}
