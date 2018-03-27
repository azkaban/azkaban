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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
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

import azkaban.reportal.util.BoundedOutputStream;
import azkaban.reportal.util.ReportalRunnerException;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;

public class ReportalPigRunner extends ReportalAbstractRunner {

  Props prop;

  public ReportalPigRunner(String jobName, Properties props) {
    super(props);
    prop = new Props();
    prop.put(props);
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void runReportal() throws Exception {
    System.out.println("Reportal Pig: Setting up Pig");

    injectAllVariables(prop.getString(PIG_SCRIPT));

    String[] args = getParams();

    System.out.println("Reportal Pig: Running pig script");
    PrintStream oldOutputStream = System.out;

    File tempOutputFile = new File("./temp.out");
    OutputStream tempOutputStream =
        new BoundedOutputStream(new BufferedOutputStream(new FileOutputStream(
            tempOutputFile)), outputCapacity);
    PrintStream printStream = new PrintStream(tempOutputStream);
    System.setOut(printStream);

    PigStats stats = PigRunner.run(args, null);

    System.setOut(oldOutputStream);

    printStream.close();

    // convert pig output to csv and write it to disk
    System.out.println("Reportal Pig: Converting output");
    InputStream tempInputStream =
        new BufferedInputStream(new FileInputStream(tempOutputFile));
    Scanner rowScanner = new Scanner(tempInputStream, StandardCharsets.UTF_8.toString());
    PrintStream csvOutputStream = new PrintStream(outputStream);
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

      File pigLogFile = new File("./");

      File[] fileList = pigLogFile.listFiles();

      for (File file : fileList) {
        if (file.isFile() && file.getName().matches("^pig_.*\\.log$")) {
          handleError(file);
        }
      }

      // see jira ticket PIG-3313. Will remove these when we use pig binary with
      // that patch.
      // /////////////////////
      System.out.println("Trying to do self kill, in case pig could not.");
      Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
      Thread[] threadArray = threadSet.toArray(new Thread[threadSet.size()]);
      for (Thread t : threadArray) {
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

  private static void handleError(File pigLog) throws Exception {
    System.out.println();
    System.out.println("====Pig logfile dump====");
    System.out.println("File: " + pigLog.getAbsolutePath());
    System.out.println();
    try {
      BufferedReader reader = new BufferedReader(
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
    } catch (FileNotFoundException e) {
      System.out.println("pig log file: " + pigLog + "  not found.");
    }
  }

  private String[] getParams() {
    ArrayList<String> list = new ArrayList<String>();

    Map<String, String> map = getPigParams();
    if (map != null) {
      for (Map.Entry<String, String> entry : map.entrySet()) {
        list.add("-param");
        list.add(StringUtils.shellQuote(
            entry.getKey() + "=" + entry.getValue(), StringUtils.SINGLE_QUOTE));
      }
    }

    // Run in local mode if filesystem is set to local.
    if (prop.getString("reportal.output.filesystem", "local").equals("local")) {
      list.add("-x");
      list.add("local");
    }

    // Register any additional Pig jars
    String additionalPigJars = prop.getString(PIG_ADDITIONAL_JARS, null);
    if (additionalPigJars != null && additionalPigJars.length() > 0) {
      list.add("-Dpig.additional.jars=" + additionalPigJars);
    }

    // Add UDF import list
    String udfImportList = prop.getString(UDF_IMPORT_LIST, null);
    if (udfImportList != null && udfImportList.length() > 0) {
      list.add("-Dudf.import.list=" + udfImportList);
    }

    // Add the script to execute
    list.add(prop.getString(PIG_SCRIPT));
    return list.toArray(new String[0]);
  }

  protected Map<String, String> getPigParams() {
    return prop.getMapByPrefix(PIG_PARAM_PREFIX);
  }

  private String transformDescriptionLine(String line) {
    int start = line.indexOf(':');
    String cleanLine = line;
    if (start != -1 && start + 3 < line.length()) {
      cleanLine = line.substring(start + 3, line.length() - 1);
    }
    return "\"" + cleanLine.replace("\"", "").replace(",", "\",\"") + "\"";
  }

  private String transformDumpLine(String line) {
    String cleanLine = line.substring(1, line.length() - 1);
    return "\"" + cleanLine.replace("\"", "").replace(",", "\",\"") + "\"";
  }

  private void injectAllVariables(String file) throws FileNotFoundException {
    // Inject variables into the script
    System.out.println("Reportal Pig: Replacing variables");
    File inputFile = new File(file);
    File outputFile = new File(file + ".bak");
    InputStream scriptInputStream =
        new BufferedInputStream(new FileInputStream(inputFile));
    Scanner rowScanner = new Scanner(scriptInputStream, StandardCharsets.UTF_8.toString());
    PrintStream scriptOutputStream =
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

  public static final String PIG_PARAM_PREFIX = "param.";
  public static final String PIG_PARAM_FILES = "paramfile";
  public static final String PIG_SCRIPT = "reportal.pig.script";
  public static final String UDF_IMPORT_LIST = "udf.import.list";
  public static final String PIG_ADDITIONAL_JARS = "pig.additional.jars";
}
