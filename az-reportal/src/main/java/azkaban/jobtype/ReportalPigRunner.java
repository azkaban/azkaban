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

import azkaban.reportal.util.BoundedOutputStream;
import azkaban.reportal.util.ReportalRunnerException;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import org.apache.pig.PigRunner;
import org.apache.pig.tools.pigstats.PigStats;

public class ReportalPigRunner extends ReportalAbstractRunner {

  public static final String PIG_PARAM_PREFIX = "param.";
  public static final String PIG_PARAM_FILES = "paramfile";
  public static final String PIG_SCRIPT = "reportal.pig.script";
  public static final String UDF_IMPORT_LIST = "udf.import.list";
  public static final String PIG_ADDITIONAL_JARS = "pig.additional.jars";
  private final String jobName;
  Props prop;

  public ReportalPigRunner(final String jobName, final Properties props) {
    super(props);
    this.jobName = jobName;
    this.prop = new Props();
    this.prop.put(props);
  }

  private static void handleError(final File pigLog) throws Exception {
    System.out.println();
    System.out.println("====Pig logfile dump====");
    System.out.println("File: " + pigLog.getAbsolutePath());
    System.out.println();
    try {
      final BufferedReader reader = new BufferedReader(
          new InputStreamReader(
              new FileInputStream(pigLog), StandardCharsets.UTF_8.toString()
          ));

      String line = reader.readLine();
      while (line != null) {
        System.out.println(line);
        line = reader.readLine();
      }
      reader.close();
      System.out.println();
      System.out.println("====End logfile dump====");
    } catch (final FileNotFoundException e) {
      System.out.println("pig log file: " + pigLog + "  not found.");
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void runReportal() throws Exception {
    System.out.println("Reportal Pig: Setting up Pig");

    injectAllVariables(this.prop.getString(PIG_SCRIPT));

    final String[] args = getParams();

    System.out.println("Reportal Pig: Running pig script");
    final PrintStream oldOutputStream = System.out;

    final File tempOutputFile = new File("./temp.out");
    final OutputStream tempOutputStream =
        new BoundedOutputStream(new BufferedOutputStream(new FileOutputStream(
            tempOutputFile)), this.outputCapacity);
    final PrintStream printStream = new PrintStream(tempOutputStream);
    System.setOut(printStream);

    final PigStats stats = PigRunner.run(args, null);

    System.setOut(oldOutputStream);

    printStream.close();

    // convert pig output to csv and write it to disk
    System.out.println("Reportal Pig: Converting output");
    final InputStream tempInputStream =
        new BufferedInputStream(new FileInputStream(tempOutputFile));
    final Scanner rowScanner = new Scanner(tempInputStream, StandardCharsets.UTF_8.toString());
    final PrintStream csvOutputStream = new PrintStream(this.outputStream);
    while (rowScanner.hasNextLine()) {
      String line = rowScanner.nextLine();
      // strip all quotes, and then quote the columns
      if (line.startsWith("(")) {
        line = transformDumpLine(line);
      } else {
        line = transformDescriptionLine(line);
      }
      csvOutputStream.println(line);
    }
    rowScanner.close();
    csvOutputStream.close();

    // Flush the temp file out
    tempOutputFile.delete();

    if (!stats.isSuccessful()) {
      System.out.println("Reportal Pig: Handling errors");

      final File pigLogFile = new File("./");

      final File[] fileList = pigLogFile.listFiles();

      for (final File file : fileList) {
        if (file.isFile() && file.getName().matches("^pig_.*\\.log$")) {
          handleError(file);
        }
      }

      // see jira ticket PIG-3313. Will remove these when we use pig binary with
      // that patch.
      // /////////////////////
      System.out.println("Trying to do self kill, in case pig could not.");
      final Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
      final Thread[] threadArray = threadSet.toArray(new Thread[threadSet.size()]);
      for (final Thread t : threadArray) {
        if (!t.isDaemon() && !t.equals(Thread.currentThread())) {
          System.out.println("Killing thread " + t);
          t.interrupt();
          t.stop();
        }
      }
      System.exit(1);
      // ////////////////////

      throw new ReportalRunnerException("Pig job failed.");
    } else {
      System.out.println("Reportal Pig: Ended successfully");
    }
  }

  private String[] getParams() {
    final ArrayList<String> list = new ArrayList<>();

    final Map<String, String> map = getPigParams();
    if (map != null) {
      for (final Map.Entry<String, String> entry : map.entrySet()) {
        list.add("-param");
        list.add(StringUtils.shellQuote(
            entry.getKey() + "=" + entry.getValue(), StringUtils.SINGLE_QUOTE));
      }
    }

    // Run in local mode if filesystem is set to local.
    if (this.prop.getString("reportal.output.filesystem", "local").equals("local")) {
      list.add("-x");
      list.add("local");
    }

    // Register any additional Pig jars
    final String additionalPigJars = this.prop.getString(PIG_ADDITIONAL_JARS, null);
    if (additionalPigJars != null && additionalPigJars.length() > 0) {
      list.add("-Dpig.additional.jars=" + additionalPigJars);
    }

    // Add UDF import list
    final String udfImportList = this.prop.getString(UDF_IMPORT_LIST, null);
    if (udfImportList != null && udfImportList.length() > 0) {
      list.add("-Dudf.import.list=" + udfImportList);
    }

    // Add the script to execute
    list.add(this.prop.getString(PIG_SCRIPT));
    return list.toArray(new String[0]);
  }

  protected Map<String, String> getPigParams() {
    return this.prop.getMapByPrefix(PIG_PARAM_PREFIX);
  }

  private String transformDescriptionLine(final String line) {
    final int start = line.indexOf(':');
    String cleanLine = line;
    if (start != -1 && start + 3 < line.length()) {
      cleanLine = line.substring(start + 3, line.length() - 1);
    }
    return "\"" + cleanLine.replace("\"", "").replace(",", "\",\"") + "\"";
  }

  private String transformDumpLine(final String line) {
    final String cleanLine = line.substring(1, line.length() - 1);
    return "\"" + cleanLine.replace("\"", "").replace(",", "\",\"") + "\"";
  }

  private void injectAllVariables(final String file) throws FileNotFoundException {
    // Inject variables into the script
    System.out.println("Reportal Pig: Replacing variables");
    final File inputFile = new File(file);

    // Creating a bak file under the root working directory, in order to copy the original pig
    // script to here with injected variables.
    final File outputFile = new File(this.jobName + ".bak");

    final InputStream scriptInputStream =
        new BufferedInputStream(new FileInputStream(inputFile));
    final Scanner rowScanner = new Scanner(scriptInputStream, StandardCharsets.UTF_8.toString());
    final PrintStream scriptOutputStream =
        new PrintStream(new BufferedOutputStream(new FileOutputStream(
            outputFile)));
    while (rowScanner.hasNextLine()) {
      String line = rowScanner.nextLine();
      line = injectVariables(line);
      scriptOutputStream.println(line);
    }
    rowScanner.close();
    scriptOutputStream.close();
    outputFile.renameTo(inputFile);
  }
}
