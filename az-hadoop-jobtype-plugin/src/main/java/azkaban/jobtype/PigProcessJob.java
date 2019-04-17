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
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.SecurityUtils;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;
import azkaban.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;


/**
 * Pig Process Job
 */
public class PigProcessJob extends JavaProcessJob {

  private static final String PIG_SCRIPT = "pig.script";
  private static final String UDF_IMPORT = "udf.import.list";
  private static final String PIG_ADDITIONAL_JARS = "pig.additional.jars";
  private static final String PIG_PARAM_PREFIX = "param.";
  private static final String PIG_PARAM_FILES = "paramfile";
  private static final String HADOOP_UGI = "hadoop.job.ugi";
  private static final String DEBUG = "debug";
  private static final String PIG_JAVA_CLASS = "org.apache.pig.Main";
  private static final String SECURE_PIG_WRAPPER = "azkaban.jobtype.SecurePigWrapper";

  public PigProcessJob(final String jobid, final Props sysProps, final Props jobProps,
      final Logger log) {
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
    final String typeGlobalJVMArgs =
        getSysProps().getString("jobtype.global.jvm.args", null);
    if (typeGlobalJVMArgs != null) {
      args += " " + typeGlobalJVMArgs;
    }

    final List<String> udfImport = getUDFImportList();
    if (udfImport != null) {
      args += " -Dudf.import.list=" + super.createArguments(udfImport, ":");
    }

    final List<String> additionalJars = getAdditionalJarsList();
    if (additionalJars.size() > 0) {
      args +=
          " -Dpig.additional.jars="
              + super.createArguments(additionalJars, ":");
    }

    final String hadoopUGI = getHadoopUGI();
    if (hadoopUGI != null) {
      args += " -Dhadoop.job.ugi=" + hadoopUGI;
    }

    if (SecurityUtils.shouldProxy(getSysProps().toProperties())) {
      info("Setting up secure proxy info for child process");
      String secure;
      final Properties p = getSysProps().toProperties();
      secure =
          " -D" + SecurityUtils.PROXY_USER + "="
              + p.getProperty(SecurityUtils.PROXY_USER);
      secure +=
          " -D" + SecurityUtils.PROXY_KEYTAB_LOCATION + "="
              + p.getProperty(SecurityUtils.PROXY_KEYTAB_LOCATION);

      secure +=
          " -D" + HadoopSecurityManager.USER_TO_PROXY + "="
              + getJobProps().get(HadoopSecurityManager.USER_TO_PROXY);

      final String extraToken = p.getProperty(SecurityUtils.OBTAIN_BINARY_TOKEN);
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
    final ArrayList<String> list = new ArrayList<>();
    final Map<String, String> map = getPigParams();
    if (map != null) {
      for (final Map.Entry<String, String> entry : map.entrySet()) {
        list.add("-param "
            + StringUtils.shellQuote(entry.getKey() + "=" + entry.getValue(),
            StringUtils.SINGLE_QUOTE));
      }
    }

    final List<String> paramFiles = getPigParamFiles();
    if (paramFiles != null) {
      for (final String paramFile : paramFiles) {
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
  @SuppressWarnings("CollectionIncompatibleType")
  protected List<String> getClassPaths() {
    final List<String> classPath = super.getClassPaths();

    // Add hadoop home setting.
    final String hadoopHome = System.getenv("HADOOP_HOME");
    if (hadoopHome == null) {
      info("HADOOP_HOME not set, using default hadoop config.");
    } else {
      info("Using hadoop config found in " + hadoopHome);
      classPath.add(new File(hadoopHome, "conf").getPath());
    }

    classPath.add(FileIOUtils.getSourcePathFromClass(Props.class));
    if (SecurityUtils.shouldProxy(getSysProps().toProperties())) {
      classPath.add(FileIOUtils.getSourcePathFromClass(SecurePigWrapper.class));
    }

    final List<String> typeClassPath =
        getSysProps().getStringList("jobtype.classpath", null, ",");
    Utils.mergeTypeClassPaths(classPath, typeClassPath, getSysProps().get("plugin.dir"));

    final List<String> typeGlobalClassPath =
        getSysProps().getStringList("jobtype.global.classpath", null, ",");
    Utils.mergeStringList(classPath, typeGlobalClassPath);
    return classPath;
  }

  private boolean getDebug() {
    return getJobProps().getBoolean(DEBUG, false);
  }

  private String getScript() {
    return getJobProps().getString(PIG_SCRIPT);
  }

  private List<String> getUDFImportList() {
    final List<String> udfImports = new ArrayList<>();
    // append typeImports
    Utils.mergeStringList(udfImports, getSysProps().getStringList(UDF_IMPORT, null, ","));
    // append jobImports
    Utils.mergeStringList(udfImports, getJobProps().getStringList(UDF_IMPORT, null, ","));
    return udfImports;
  }

  private List<String> getAdditionalJarsList() {
    final List<String> additionalJars = new ArrayList<>();
    // append typeJars
    Utils.mergeStringList(additionalJars,
        getSysProps().getStringList(PIG_ADDITIONAL_JARS, null, ","));
    // append jobJars
    Utils.mergeStringList(additionalJars,
        getJobProps().getStringList(PIG_ADDITIONAL_JARS, null, ","));

    return additionalJars;
  }

  private String getHadoopUGI() {
    return getJobProps().getString(HADOOP_UGI, null);
  }

  private Map<String, String> getPigParams() {
    return getJobProps().getMapByPrefix(PIG_PARAM_PREFIX);
  }

  private List<String> getPigParamFiles() {
    return getJobProps().getStringList(PIG_PARAM_FILES, null, ",");
  }
}
