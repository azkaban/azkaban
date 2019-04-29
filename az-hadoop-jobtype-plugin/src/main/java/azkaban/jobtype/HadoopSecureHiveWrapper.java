/*
 * Copyright 2012 LinkedIn, Inc
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

import static azkaban.security.commons.SecurityUtils.MAPREDUCE_JOB_CREDENTIALS_BINARY;
import static azkaban.utils.StringUtils.DOUBLE_QUOTE;
import static azkaban.utils.StringUtils.SINGLE_QUOTE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEAUXJARS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORECONNECTURLKEY;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.cli.OptionsProcessor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import azkaban.jobtype.hiveutils.HiveQueryExecutionException;
import azkaban.utils.Props;


public class HadoopSecureHiveWrapper {

  private static final String DOUBLE_QUOTE_STRING = Character
      .toString(DOUBLE_QUOTE);
  private static final String SINGLE_QUOTE_STRING = Character
      .toString(SINGLE_QUOTE);

  private static final Logger logger = Logger.getRootLogger();

  private static CliSessionState ss;
  private static String hiveScript;

  public static void main(final String[] args) throws Exception {

    Properties jobProps = HadoopSecureWrapperUtils.loadAzkabanProps();
    HadoopConfigurationInjector.injectResources(new Props(null, jobProps));

    hiveScript = jobProps.getProperty("hive.script");

    if (HadoopSecureWrapperUtils.shouldProxy(jobProps)) {
      String tokenFile = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
      UserGroupInformation proxyUser =
          HadoopSecureWrapperUtils.setupProxyUser(jobProps, tokenFile, logger);
      proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          runHive(args);
          return null;
        }
      });
    } else {
      runHive(args);
    }
  }

  public static void runHive(String[] args) throws Exception {

    final HiveConf hiveConf = new HiveConf(SessionState.class);

    populateHiveConf(hiveConf, args);

    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      System.out.println("Setting hadoop tokens ... ");
      hiveConf.set(MAPREDUCE_JOB_CREDENTIALS_BINARY,
          System.getenv(HADOOP_TOKEN_FILE_LOCATION));
      System.setProperty(MAPREDUCE_JOB_CREDENTIALS_BINARY,
          System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }

    logger.info("HiveConf = " + hiveConf);
    logger.info("According to the conf, we're talking to the Hive hosted at: "
        + HiveConf.getVar(hiveConf, METASTORECONNECTURLKEY));

    String orig = HiveConf.getVar(hiveConf, HIVEAUXJARS);
    String expanded = expandHiveAuxJarsPath(orig);
    if (orig == null || orig.equals(expanded)) {
      logger.info("Hive aux jars variable not expanded");
    } else {
      logger.info("Expanded aux jars variable from [" + orig + "] to ["
          + expanded + "]");
      HiveConf.setVar(hiveConf, HIVEAUXJARS, expanded);
    }

    OptionsProcessor op = new OptionsProcessor();

    if (!op.process_stage1(new String[]{})) {
      throw new IllegalArgumentException("Can't process empty args?!?");
    }

    // hadoop-20 and above - we need to augment classpath using hiveconf
    // components
    // see also: code in ExecDriver.java
    ClassLoader loader = hiveConf.getClassLoader();
    String auxJars = HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVEAUXJARS);
    logger.info("Got auxJars = " + auxJars);

    if (StringUtils.isNotBlank(auxJars)) {
      loader =
          Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","));
    }
    hiveConf.setClassLoader(loader);
    Thread.currentThread().setContextClassLoader(loader);

    // See https://issues.apache.org/jira/browse/HIVE-1411
    hiveConf.set("datanucleus.plugin.pluginRegistryBundleCheck", "LOG");

    // to force hive to use the jobclient to submit the job, never using
    // HADOOPBIN (to do localmode)
    hiveConf.setBoolean("hive.exec.mode.local.auto", false);

    ss = new CliSessionState(hiveConf);
    SessionState.start(ss);

    logger.info("SessionState = " + ss);
    ss.out = System.out;
    ss.err = System.err;
    ss.in = System.in;

    if (!op.process_stage2(ss)) {
      throw new IllegalArgumentException(
          "Can't process arguments from session state");
    }

    logger.info("Executing query: " + hiveScript);

    CliDriver cli = new CliDriver();
    Map<String, String> hiveVarMap = getHiveVarMap(args);

    logger.info("hiveVarMap: " + hiveVarMap);

    if (!hiveVarMap.isEmpty()) {
      cli.setHiveVariables(getHiveVarMap(args));
    }

    int returnCode = cli.processFile(hiveScript);
    if (returnCode != 0) {
      logger.warn("Got exception " + returnCode + " from line: " + hiveScript);
      throw new HiveQueryExecutionException(returnCode, hiveScript);
    }
  }

  /**
   * Normally hive.aux.jars.path is expanded from just being a path to the full
   * list of files in the directory by the hive shell script. Since we normally
   * won't be running from the script, it's up to us to do that work here. We
   * use a heuristic that if there is no occurrence of ".jar" in the original,
   * it needs expansion. Otherwise it's already been done for us.
   *
   * Also, surround the files with uri niceities.
   */
  static String expandHiveAuxJarsPath(String original) throws IOException {
    if (original == null || original.contains(".jar")) {
      return original;
    }

    File[] files = new File(original).listFiles();

    if (files == null || files.length == 0) {
      logger
          .info("No files in to expand in aux jar path. Returning original parameter");
      return original;
    }

    return filesToURIString(files);

  }

  static String filesToURIString(File[] files) throws IOException {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < files.length; i++) {
      sb.append("file:///").append(files[i].getCanonicalPath());
      if (i != files.length - 1) {
        sb.append(",");
      }
    }

    return sb.toString();
  }

  /**
   * Extract hiveconf from command line arguments and populate them into
   * HiveConf
   *
   * An example: -hiveconf 'zipcode=10', -hiveconf hive.root.logger=INFO,console
   */
  private static void populateHiveConf(HiveConf hiveConf, String[] args) {

    if (args == null) {
      return;
    }

    int index = 0;
    for (; index < args.length; index++) {
      if ("-hiveconf".equals(args[index])) {
        String hiveConfParam = stripSingleDoubleQuote(args[++index]);

        String[] tokens = hiveConfParam.split("=");
        if (tokens.length == 2) {
          String name = tokens[0];
          String value = tokens[1];
          logger.info("Setting: " + name + "=" + value + " to hiveConf");
          hiveConf.set(name, value);
        } else {
          logger.warn("Invalid hiveconf: " + hiveConfParam);
        }
      }
    }
  }

  static Map<String, String> getHiveVarMap(String[] args) {

    if (args == null) {
      return Collections.emptyMap();
    }

    Map<String, String> hiveVarMap = new HashMap<String, String>();
    for (int index = 0; index < args.length; index++) {
      if ("-hivevar".equals(args[index])) {
        String hiveVarParam = stripSingleDoubleQuote(args[++index]);
        // Separate the parameter string at its first occurence of "="
        int gap = hiveVarParam.indexOf("=");
        if (gap == -1) {
          logger.warn("Invalid hivevar: " + hiveVarParam);
          continue;
        }
        String name = hiveVarParam.substring(0, gap);
        String value = hiveVarParam.substring(gap + 1);
        logger.info("Setting hivevar: " + name + "=" + value);
        hiveVarMap.put(name, value);
      }
    }
    return hiveVarMap;
  }

  /**
   * Strip single quote or double quote at either end of the string
   *
   * @return string with w/o leading or trailing single or double quote
   */
  private static String stripSingleDoubleQuote(String input) {
    if (StringUtils.isEmpty(input)) {
      return input;
    }

    if (input.startsWith(SINGLE_QUOTE_STRING)
        || input.startsWith(DOUBLE_QUOTE_STRING)) {
      input = input.substring(1);
    }

    if (input.endsWith(SINGLE_QUOTE_STRING)
        || input.endsWith(DOUBLE_QUOTE_STRING)) {
      input = input.substring(0, input.length() - 1);
    }

    return input;
  }
}
