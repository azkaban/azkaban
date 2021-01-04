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
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.apache.pig.PigRunner;
import azkaban.flow.CommonJobProperties;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;
import azkaban.utils.Utils;


/*
 * need lib:
 * apache pig
 * hadoop-core*.jar
 * HadoopSecurePigWrapper
 * HadoopSecurityManager(corresponding version with hadoop)
 * abandon support for pig 0.8 and prior versions. don't see a use case here.
 */
public class HadoopPigJob extends AbstractHadoopJavaProcessJob {

  private static final String PIG_SCRIPT = "pig.script";
  private static final String UDF_IMPORT = "udf.import.list";
  private static final String PIG_ADDITIONAL_JARS = "pig.additional.jars";
  private static final String DEFAULT_PIG_ADDITIONAL_JARS = "default.pig.additional.jars";
  private static final String PIG_PARAM_PREFIX = "param.";
  private static final String PIG_PARAM_FILES = "paramfile";
  private static final String HADOOP_UGI = "hadoop.job.ugi";
  private static final String DEBUG = "debug";
  private static final String RAMP_SCRIPT_DIR = "ramp";
  private static final String STATEMENT_TERMINATOR = ";";
  private static final String REGISTER_STATEMENT_FORMATTER = "REGISTER '%s'; ";
  private static final String REGISTER_STATEMENT_PATTERN = "(?i)\\s*register\\s+[\"|'](\\S*/)*%s\\W+\\S*.jar[\"|']\\s*;\\s*";

  private static String HADOOP_SECURE_PIG_WRAPPER = "azkaban.jobtype.HadoopSecurePigWrapper";

  private final boolean userPigJar;
  private File pigLogFile = null;

  public HadoopPigJob(String jobid, Props sysProps, Props jobProps, Logger log) {
    super(jobid, sysProps, jobProps, log);
    getJobProps().put(CommonJobProperties.JOB_ID, jobid);
    HADOOP_SECURE_PIG_WRAPPER = HadoopSecurePigWrapper.class.getName();
    userPigJar = getJobProps().getBoolean("use.user.pig.jar", false);
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
    return HADOOP_SECURE_PIG_WRAPPER;
  }

  @Override
  protected String getJVMArguments() {
    String args = super.getJVMArguments();

    String typeGlobalJVMArgs =
        getSysProps().getString(HadoopJobUtils.JOBTYPE_GLOBAL_JVM_ARGS, null);
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

    args += getJVMProxySecureArgument();

    return args;
  }

  @Override
  protected String getMainArguments() {
    List<String> list = new ArrayList<>();
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
      getJobProps().put("env." + "PIG_LOG_FILE", pigLogFile.getAbsolutePath());
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (pigLogFile != null) {
      list.add("-logfile " + pigLogFile.getAbsolutePath());
    }

    Map<Pattern, String> rampRegisterItems = getRampItems(JavaProcessJob.DEPENDENCY_REG_RAMP_PROP_PREFIX)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(
            entry -> Pattern.compile(String.format(REGISTER_STATEMENT_PATTERN, entry.getKey()), Pattern.CASE_INSENSITIVE),
            entry -> String.format(REGISTER_STATEMENT_FORMATTER, entry.getValue())
        ));

    if (rampRegisterItems.isEmpty()) {
     list.add(getScript());
    } else {
      try {
        File srcFile = new File(getScriptAbsolutePath());
        File dstFile = new File(getRunnableScriptAbsolutePath());
        Path dstPath = Paths.get(getRunnableScriptDir());
        getLog().info(String.format("[Ramp Modification Start] [srcFile = %s, dstFile = %s, dstPath = %s]",
            srcFile.toPath().toAbsolutePath().toString(),
            dstFile.toPath().toAbsolutePath().toString(),
            dstPath.toString()));
        if (!Files.exists(dstPath)) {
          Files.createDirectories(dstPath);
          getLog().info(String.format("[Ramp Modification Destination Directory Created. %s]",
              dstPath.toAbsolutePath().toString()));
        }
        dstFile.createNewFile();
        getLog().info(String.format("[Ramp Modification Destination File Created. %s]",
            dstFile.toPath().toAbsolutePath().toString()));
        getLog().info(String.format("[Ramp Modify Script File] : old = %s, new = %s",
            srcFile.getAbsolutePath(), dstFile.getAbsolutePath()));
        copyAndModifyScript(srcFile, dstFile, rampRegisterItems);
        getLog().info(String.format("[Ramp Modification End] [dstFile = %s]",
            dstFile.toPath().toAbsolutePath().toString()));
        list.add(getRunnableScript());
      } catch (IOException e) {
        getLog().error(e);
        getLog().info("[Ramp cannot successfully modify the script, Failover to the baseline.]");
        list.add(getScript());
      }
    }

    return StringUtils.join((Collection<String>) list, " ");
  }

  /**
   * Copy Pig Script from source to destination and update REGISTER statements based on the map of ramp items
   */
  private void copyAndModifyScript(File source, File dest, Map<Pattern, String> rampRegisterItems) throws IOException {
    BufferedReader bufferedReader = null;
    PrintWriter printWriter = null;
    bufferedReader = Files.newBufferedReader(source.toPath(), Charset.defaultCharset());
    printWriter =  new PrintWriter(Files.newBufferedWriter(dest.toPath(), Charset.defaultCharset()));
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      printWriter.println(replaceRegisterStatements(line, rampRegisterItems));
    }
    bufferedReader.close();
    printWriter.close();
  }

  /**
   * Replace Register statement to a particular ramp value
   */
  private String replaceRegisterStatements(String text, Map<Pattern, String> rampRegisterItems) {
    StringBuilder sb = new StringBuilder();
    int start = 0;
    int end = text.length();
    int idx = 0;
    String statement = null;
    while(start < end && idx >= 0) {
      idx = text.indexOf(STATEMENT_TERMINATOR, start);
      if (idx > 0) {
        statement = text.substring(start, idx+1);
      } else {
        statement = text.substring(start);
      }
      final String value = statement;
      sb.append(
          rampRegisterItems
              .entrySet()
              .stream()
              .filter(entry -> entry.getKey().matcher(value).matches())
              .map(Map.Entry::getValue)
              .findFirst()
              .orElse(statement)
      );
      start = idx + 1;
    }

    String modifiedText = sb.toString();

    if (!modifiedText.equals(text)) {
      getLog().info(String.format("[RAMP Statement] : Original = %s, Modified = %s", text, modifiedText));
    }
    return modifiedText;
  }

  @Override
  protected List<String> getClassPaths() {

    List<String> classPath = super.getClassPaths();

    classPath.addAll(getAzkabanCommonClassPaths());

    classPath.add(HadoopConfigurationInjector.getPath(getJobProps(),
        getWorkingDirectory()));

    // assuming pig 0.8 and up
    if (!userPigJar) {
      classPath.add(FileIOUtils.getSourcePathFromClass(PigRunner.class));
    }

    // merging classpaths from plugin.properties
    Utils.mergeTypeClassPaths(classPath,
        getJobProps().getStringList("jobtype.classpath", null, ","),
        getSysProps().get("plugin.dir"));

    mergeSysTypeClassPaths(classPath);

    Utils.mergeStringList(
        classPath,
        getRampItems(JavaProcessJob.DEPENDENCY_CLS_RAMP_PROP_PREFIX)
            .entrySet()
            .stream()
            .map(Map.Entry::getValue)
            .collect(Collectors.toList())
    );

    return classPath;
  }

  private boolean getDebug() {
    return getJobProps().getBoolean(DEBUG, false);
  }

  private String getScript() {
    return getJobProps().getString(PIG_SCRIPT);
  }

  private String getRunnableScript() {
    return RAMP_SCRIPT_DIR + "/" + getScript();
  }

  private String getScriptAbsolutePath() {
    return getWorkingDirectory() + "/" + getScript();
  }

  private String getRunnableScriptDir() {
    return getWorkingDirectory() + "/" + RAMP_SCRIPT_DIR;
  }
  private String getRunnableScriptAbsolutePath() {
    return getWorkingDirectory() + "/" + getRunnableScript();
  }

  private List<String> getUDFImportList() {
    List<String> udfImports = new ArrayList<>();
    //append typeImports
    Utils.mergeStringList(udfImports, getSysProps().getStringList(UDF_IMPORT, null, ","));
    //append jobImports
    Utils.mergeStringList(udfImports, getJobProps().getStringList(UDF_IMPORT, null, ","));
    return udfImports;
  }

  /**
   * Merging all additional jars first from user specified property
   * and private.properties (in the jobtype property) for additionalJarProperty
   * TODO kunkun-tang: A refactor is necessary here. Recommend using Java Optional to better handle
   * parsing exceptions.
   */
  private List<String> getAdditionalJarsList() {

    List<String> wholeAdditionalJarsList = new ArrayList<>();

    List<String> jobAdditionalJars =
        getJobProps().getStringList(PIG_ADDITIONAL_JARS, null, ",");

    List<String> jobDefinedDefaultJars =
        getJobProps().getStringList(DEFAULT_PIG_ADDITIONAL_JARS, null, ",");
    List<String> systemDefinedDefaultJars =
        getSysProps().getStringList(DEFAULT_PIG_ADDITIONAL_JARS, null, ",");

    /*
      if user defines the custom default pig Jar, we only incorporate the user
      settings; otherwise, only when system configurations have it, we add the system
      additional jar settings. We don't accept both at the same time.
     */
    if (jobAdditionalJars != null) {
      wholeAdditionalJarsList.addAll(jobAdditionalJars);
    }

    if (jobDefinedDefaultJars != null) {
      wholeAdditionalJarsList.addAll(jobDefinedDefaultJars);
    } else if (systemDefinedDefaultJars != null) {
      wholeAdditionalJarsList.addAll(systemDefinedDefaultJars);
    }
    return wholeAdditionalJarsList;
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

  /**
   * This cancel method, in addition to the default canceling behavior, also
   * kills the MR jobs launched by Pig on Hadoop
   */
  @Override
  public void cancel() throws InterruptedException {
    super.cancel();

    info("Cancel called.  Killing the Pig launched MR jobs on the cluster");

    getHadoopProxy().killAllSpawnedHadoopJobs(getJobProps(), getLog());
  }
}
