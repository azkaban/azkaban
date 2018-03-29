/*
 * Copyright 2018 LinkedIn Corp.
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
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import azkaban.reportal.util.BoundedOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.cli.OptionsProcessor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState;

public class ReportalHiveRunner extends ReportalAbstractRunner {

  public ReportalHiveRunner(final String jobName, final Properties props) {
    super(props);
  }

  /**
   * Normally hive.aux.jars.path is expanded from just being a path to the full
   * list of files in the directory by the hive shell script. Since we normally
   * won't be running from the script, it's up to us to do that work here. We
   * use a heuristic that if there is no occurrence of ".jar" in the original,
   * it needs expansion. Otherwise it's already been done for us. Also, surround
   * the files with uri niceities.
   */
  static String expandHiveAuxJarsPath(final String original) throws IOException {
    if (original == null || original.endsWith(".jar")) {
      return original;
    }

    final File[] files = new File(original).listFiles();

    if (files == null || files.length == 0) {
      return original;
    }

    return filesToURIString(files);

  }

  static String filesToURIString(final File[] files) throws IOException {
    final StringBuffer sb = new StringBuffer();
    for (int i = 0; i < files.length; i++) {
      sb.append("file:///").append(files[i].getCanonicalPath());
      if (i != files.length - 1) {
        sb.append(",");
      }
    }

    return sb.toString();
  }

  @Override
  protected void runReportal() throws Exception {
    System.out.println("Reportal Hive: Setting up Hive");
    final HiveConf conf = new HiveConf(SessionState.class);

    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      conf.set(MAPREDUCE_JOB_CREDENTIALS_BINARY,
          System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }

    final File tempTSVFile = new File("./temp.tsv");
    final OutputStream tsvTempOutputStream =
        new BoundedOutputStream(new BufferedOutputStream(new FileOutputStream(
            tempTSVFile)), this.outputCapacity);
    final PrintStream logOut = System.out;

    final String orig = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);

    final CliSessionState sessionState = new CliSessionState(conf);
    sessionState.in = System.in;
    sessionState.out = new PrintStream(tsvTempOutputStream, true, "UTF-8");
    sessionState.err = new PrintStream(logOut, true, "UTF-8");

    final OptionsProcessor oproc = new OptionsProcessor();

    // Feed in Hive Args
    final String[] args = buildHiveArgs();
    if (!oproc.process_stage1(args)) {
      throw new Exception("unable to parse options stage 1");
    }

    if (!oproc.process_stage2(sessionState)) {
      throw new Exception("unable to parse options stage 2");
    }

    // Set all properties specified via command line
    for (final Map.Entry<Object, Object> item : sessionState.cmdProperties.entrySet()) {
      conf.set((String) item.getKey(), (String) item.getValue());
    }

    SessionState.start(sessionState);

    final String expanded = expandHiveAuxJarsPath(orig);
    if (orig == null || orig.equals(expanded)) {
      System.out.println("Hive aux jars variable not expanded");
    } else {
      System.out.println("Expanded aux jars variable from [" + orig + "] to ["
          + expanded + "]");
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVEAUXJARS, expanded);
    }

    // hadoop-20 and above - we need to augment classpath using hiveconf
    // components
    // see also: code in ExecDriver.java
    ClassLoader loader = conf.getClassLoader();
    final String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);

    System.out.println("Got auxJars = " + auxJars);

    if (StringUtils.isNotBlank(auxJars)) {
      loader = Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","));
    }
    conf.setClassLoader(loader);
    Thread.currentThread().setContextClassLoader(loader);

    final CliDriver cli = new CliDriver();
    int returnValue = 0;
    String prefix = "";

    returnValue = cli.processLine("set hive.cli.print.header=true;");
    final String[] queries = this.jobQuery.split("\n");
    for (String line : queries) {
      if (!prefix.isEmpty()) {
        prefix += '\n';
      }
      if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
        line = prefix + line;
        line = injectVariables(line);
        System.out.println("Reportal Hive: Running Hive Query: " + line);
        returnValue = cli.processLine(line);
        prefix = "";
      } else {
        prefix = prefix + line;
        continue;
      }
    }

    tsvTempOutputStream.close();

    // convert tsv to csv and write it do disk
    System.out.println("Reportal Hive: Converting output");
    final InputStream tsvTempInputStream =
        new BufferedInputStream(new FileInputStream(tempTSVFile));
    final Scanner rowScanner = new Scanner(tsvTempInputStream, StandardCharsets.UTF_8.toString());
    final PrintStream csvOutputStream = new PrintStream(this.outputStream);
    while (rowScanner.hasNextLine()) {
      final String tsvLine = rowScanner.nextLine();
      // strip all quotes, and then quote the columns
      csvOutputStream.println("\""
          + tsvLine.replace("\"", "").replace("\t", "\",\"") + "\"");
    }
    rowScanner.close();
    csvOutputStream.close();

    // Flush the temp file out
    tempTSVFile.delete();

    if (returnValue != 0) {
      throw new Exception("Hive query finished with a non zero return code");
    }

    System.out.println("Reportal Hive: Ended successfully");
  }

  private String[] buildHiveArgs() {
    final List<String> confBuilder = new ArrayList<>();

    if (this.proxyUser != null) {
      confBuilder.add("hive.exec.scratchdir=/tmp/hive-" + this.proxyUser);
    }

    if (this.jobTitle != null) {
      confBuilder.add("mapred.job.name=\"Reportal: " + this.jobTitle + "\"");
    }

    confBuilder.add("mapreduce.job.complete.cancel.delegation.tokens=false");

    final String[] args = new String[confBuilder.size() * 2];

    for (int i = 0; i < confBuilder.size(); i++) {
      args[i * 2] = "--hiveconf";
      args[i * 2 + 1] = confBuilder.get(i);
    }

    return args;
  }
}
