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

import azkaban.jobExecutor.JavaProcessJob;

import azkaban.security.commons.SecurityUtils;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;

public class PigProcessJob extends JavaProcessJob {

  public static final String PIG_SCRIPT = "pig.script";
  public static final String UDF_IMPORT = "udf.import.list";
  public static final String PIG_ADDITIONAL_JARS = "pig.additional.jars";
  public static final String PIG_PARAM_PREFIX = "param.";
  public static final String PIG_PARAM_FILES = "paramfile";
  public static final String HADOOP_UGI = "hadoop.job.ugi";
  public static final String DEBUG = "debug";

  public static final String PIG_JAVA_CLASS = "org.apache.pig.Main";
  public static final String SECURE_PIG_WRAPPER =
      "azkaban.jobtype.SecurePigWrapper";

  public PigProcessJob(String jobid, Props sysProps, Props jobProps, Logger log) {
    super(jobid, sysProps, new Props(sysProps, jobProps), log);
  }

  @Override
  protected String getJavaClass() {
    return SecurityUtils.shouldProxy(getSysProps().toProperties())
        ? SECURE_PIG_WRAPPER
        : PIG_JAVA_CLASS;
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
    if (udfImport != null) {
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

    if (SecurityUtils.shouldProxy(getSysProps().toProperties())) {
      info("Setting up secure proxy info for child process");
      String secure;
      Properties p = getSysProps().toProperties();
      secure =
          " -D" + SecurityUtils.PROXY_USER + "="
              + p.getProperty(SecurityUtils.PROXY_USER);
      secure +=
          " -D" + SecurityUtils.PROXY_KEYTAB_LOCATION + "="
              + p.getProperty(SecurityUtils.PROXY_KEYTAB_LOCATION);

      secure +=
          " -D" + SecurityUtils.TO_PROXY + "="
              + getJobProps().get(SecurityUtils.TO_PROXY);

      String extraToken = p.getProperty(SecurityUtils.OBTAIN_BINARY_TOKEN);
      if (extraToken != null) {
        secure += " -D" + SecurityUtils.OBTAIN_BINARY_TOKEN + "=" + extraToken;
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

    list.add(getScript());

    return StringUtils.join((Collection<String>) list, " ");
  }

  @Override
  protected List<String> getClassPaths() {
    List<String> classPath = super.getClassPaths();

    // Add hadoop home setting.
    String hadoopHome = System.getenv("HADOOP_HOME");
    if (hadoopHome == null) {
      info("HADOOP_HOME not set, using default hadoop config.");
    } else {
      info("Using hadoop config found in " + hadoopHome);
      classPath.add(new File(hadoopHome, "conf").getPath());
    }

    classPath.add(getSourcePathFromClass(Props.class));
    if (SecurityUtils.shouldProxy(getSysProps().toProperties())) {
      classPath.add(getSourcePathFromClass(SecurePigWrapper.class));
    }

    List<String> typeClassPath =
        getSysProps().getStringList("jobtype.classpath", null, ",");
    if (typeClassPath != null) {
      // fill in this when load this jobtype
      String pluginDir = getSysProps().get("plugin.dir");
      for (String jar : typeClassPath) {
        File jarFile = new File(jar);
        if (!jarFile.isAbsolute()) {
          jarFile = new File(pluginDir + File.separatorChar + jar);
        }

        if (!classPath.contains(jarFile.getAbsoluteFile())) {
          classPath.add(jarFile.getAbsolutePath());
        }
      }
    }

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
    List<String> typeJars =
        getSysProps().getStringList(PIG_ADDITIONAL_JARS, null, ",");
    List<String> jobJars =
        getJobProps().getStringList(PIG_ADDITIONAL_JARS, null, ",");
    if (typeJars != null) {
      additionalJars.addAll(typeJars);
    }
    if (jobJars != null) {
      additionalJars.addAll(jobJars);
    }
    return additionalJars;
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
}
