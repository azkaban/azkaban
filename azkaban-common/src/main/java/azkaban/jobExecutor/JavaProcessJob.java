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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import azkaban.project.DirectoryFlowLoader;
import azkaban.server.AzkabanServer;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.Utils;

public class JavaProcessJob extends ProcessJob {
  public static final String CLASSPATH = "classpath";
  public static final String GLOBAL_CLASSPATH = "global.classpaths";
  public static final String JAVA_CLASS = "java.class";
  public static final String INITIAL_MEMORY_SIZE = "Xms";
  public static final String MAX_MEMORY_SIZE = "Xmx";
  public static final String MAIN_ARGS = "main.args";
  public static final String JVM_PARAMS = "jvm.args";
  public static final String GLOBAL_JVM_PARAMS = "global.jvm.args";

  public static final String DEFAULT_INITIAL_MEMORY_SIZE = "64M";
  public static final String DEFAULT_MAX_MEMORY_SIZE = "256M";

  public static String JAVA_COMMAND = "java";

  public JavaProcessJob(String jobid, Props sysProps, Props jobProps,
      Logger logger) {
    super(jobid, sysProps, jobProps, logger);
  }

  @Override
  protected List<String> getCommandList() {
    ArrayList<String> list = new ArrayList<String>();
    list.add(createCommandLine());
    return list;
  }

  protected String createCommandLine() {
    String command = JAVA_COMMAND + " ";
    command += getJVMArguments() + " ";
    command += "-Xms" + getInitialMemorySize() + " ";
    command += "-Xmx" + getMaxMemorySize() + " ";
    command += "-cp " + createArguments(getClassPaths(), ":") + " ";
    command += getJavaClass() + " ";
    command += getMainArguments();

    return command;
  }

  protected String getJavaClass() {
    return getJobProps().getString(JAVA_CLASS);
  }

  protected String getClassPathParam() {
    List<String> classPath = getClassPaths();
    if (classPath == null || classPath.size() == 0) {
      return "";
    }

    return "-cp " + createArguments(classPath, ":") + " ";
  }

  protected List<String> getClassPaths() {

    List<String> classPaths = getJobProps().getStringList(CLASSPATH, null, ",");

    ArrayList<String> classpathList = new ArrayList<String>();
    // Adding global properties used system wide.
    if (getJobProps().containsKey(GLOBAL_CLASSPATH)) {
      List<String> globalClasspath =
          getJobProps().getStringList(GLOBAL_CLASSPATH);
      for (String global : globalClasspath) {
        getLog().info("Adding to global classpath:" + global);
        classpathList.add(global);
      }
    }

    if (classPaths == null) {
      File path = new File(getPath());
      // File parent = path.getParentFile();
      getLog().info(
          "No classpath specified. Trying to load classes from " + path);

      if (path != null) {
        for (File file : path.listFiles()) {
          if (file.getName().endsWith(".jar")) {
            // log.info("Adding to classpath:" + file.getName());
            classpathList.add(file.getName());
          }
        }
      }
    } else {
      classpathList.addAll(classPaths);
    }

    return classpathList;
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
    String globalJVMArgs = getJobProps().getString(GLOBAL_JVM_PARAMS, null);

    if (globalJVMArgs == null) {
      return getJobProps().getString(JVM_PARAMS, "");
    }

    return globalJVMArgs + " " + getJobProps().getString(JVM_PARAMS, "");
  }

  protected String createArguments(List<String> arguments, String separator) {
    if (arguments != null && arguments.size() > 0) {
      String param = "";
      for (String arg : arguments) {
        param += arg + separator;
      }

      return param.substring(0, param.length() - 1);
    }

    return "";
  }

  protected Pair<Long, Long> getProcMemoryRequirement() throws Exception {
    String strXms = getInitialMemorySize();
    String strXmx = getMaxMemorySize();
    long xms = Utils.parseMemString(strXms);
    long xmx = Utils.parseMemString(strXmx);

    Props azkabanProperties = AzkabanServer.getAzkabanProperties();
    if (azkabanProperties != null) {
      String maxXms = azkabanProperties.getString(DirectoryFlowLoader.JOB_MAX_XMS, DirectoryFlowLoader.MAX_XMS_DEFAULT);
      String maxXmx = azkabanProperties.getString(DirectoryFlowLoader.JOB_MAX_XMX, DirectoryFlowLoader.MAX_XMX_DEFAULT);
      long sizeMaxXms = Utils.parseMemString(maxXms);
      long sizeMaxXmx = Utils.parseMemString(maxXmx);

      if (xms > sizeMaxXms) {
        throw new Exception(String.format("%s: Xms value has exceeded the allowed limit (max Xms = %s)",
                getId(), maxXms));
      }

      if (xmx > sizeMaxXmx) {
        throw new Exception(String.format("%s: Xmx value has exceeded the allowed limit (max Xmx = %s)",
                getId(), maxXmx));
      }
    }

    return new Pair<Long, Long>(xms, xmx);
  }
}
