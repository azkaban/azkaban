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

import azkaban.flow.CommonJobProperties;
import azkaban.reportal.util.BoundedOutputStream;
import azkaban.reportal.util.ReportalRunnerException;
import azkaban.utils.Props;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.hadoop.conf.Configuration;

public abstract class ReportalAbstractRunner {

  private static final String REPORTAL_VARIABLE_PREFIX = "reportal.variable.";
  protected Props props;
  protected OutputStream outputStream;
  protected String proxyUser;
  protected String jobQuery;
  protected String jobTitle;
  protected String reportalTitle;
  protected String reportalStorageUser;
  protected int outputCapacity;
  protected Map<String, String> variables = new HashMap<>();

  public ReportalAbstractRunner(final Properties props) {
    final Props prop = new Props();
    prop.put(props);
    this.props = prop;
  }

  public void run() throws Exception {
    System.out.println("Reportal: Setting up environment");
    System.out.println("Reportal: " + this.props.toString());

    // Check the properties file
    if (this.props == null) {
      throw new ReportalRunnerException("Properties file not loaded correctly.");
    }

    // Get the hadoop token
    final Configuration conf = new Configuration();
    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      conf.set(MAPREDUCE_JOB_CREDENTIALS_BINARY,
          System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }

    // Get properties
    final String execId = this.props.getString(CommonJobProperties.EXEC_ID);
    this.outputCapacity = this.props.getInt("reportal.output.capacity", 10 * 1024 * 1024);
    this.proxyUser = this.props.getString("reportal.proxy.user");
    this.jobQuery = this.props.getString("reportal.job.query");
    this.jobTitle = this.props.getString("reportal.job.title");
    this.reportalTitle = this.props.getString("reportal.title");
    this.reportalStorageUser = this.props.getString("reportal.storage.user", "reportal");
    final Map<String, String> reportalVariables =
        this.props.getMapByPrefix(REPORTAL_VARIABLE_PREFIX);

    // Parse variables
    for (final Entry<String, String> entry : reportalVariables.entrySet()) {
      if (entry.getKey().endsWith("from")) {
        final String fromValue = entry.getValue();
        final String toKey =
            entry.getKey().substring(0, entry.getKey().length() - 4) + "to";
        final String toValue = reportalVariables.get(toKey);
        if (toValue != null) {
          this.variables.put(fromValue, toValue);
        }
      }
    }

    // Built-in variables
    this.variables.put("run_id", execId);
    this.variables.put("sys_date", Long.toString(System.currentTimeMillis() / 1000));

    final Calendar cal = Calendar.getInstance();
    final Date date = new Date();
    cal.setTime(date);

    final String timeZone = this.props.getString("reportal.default.timezone", "UTC");
    TimeZone.setDefault(TimeZone.getTimeZone(timeZone));

    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    final SimpleDateFormat hourFormat = new SimpleDateFormat("yyyy-MM-dd-HH");

    this.variables.put("hive_current_hour", hourFormat.format(cal.getTime()));
    this.variables.put("hive_current_day", dateFormat.format(cal.getTime()));
    cal.add(Calendar.HOUR, -1);
    this.variables.put("hive_last_hour", hourFormat.format(cal.getTime()));
    cal.add(Calendar.HOUR, 1);
    cal.add(Calendar.DATE, -1);
    this.variables.put("hive_yesterday", dateFormat.format(cal.getTime()));
    cal.add(Calendar.DATE, -6);
    this.variables.put("hive_last_seven_days", dateFormat.format(cal.getTime()));
    cal.add(Calendar.DATE, -1);
    this.variables.put("hive_last_eight_days", dateFormat.format(cal.getTime()));
    this.variables.put("owner", this.proxyUser);
    this.variables.put("title", this.reportalTitle);

    // Props debug
    System.out.println("Reportal Variables:");
    for (final Entry<String, String> data : this.variables.entrySet()) {
      System.out.println(data.getKey() + " -> " + data.getValue());
    }

    if (requiresOutput()) {
      // Get output stream to data
      final String locationTemp =
          ("./reportal/" + this.jobTitle + ".csv").replace("//", "/");
      final File tempOutput = new File(locationTemp);
      tempOutput.getParentFile().mkdirs();
      tempOutput.createNewFile();
      this.outputStream =
          new BoundedOutputStream(new BufferedOutputStream(
              new FileOutputStream(tempOutput)), this.outputCapacity);

      // Run the reportal
      runReportal();

      // Cleanup the reportal
      try {
        this.outputStream.close();
      } catch (final IOException e) {
        // We can safely ignore this exception since we're just making sure the
        // stream is closed.
      }
    } else {
      runReportal();
    }
  }

  protected abstract void runReportal() throws Exception;

  protected boolean requiresOutput() {
    return true;
  }

  protected String injectVariables(String line) {
    for (final Entry<String, String> entry : this.variables.entrySet()) {
      line =
          line.replace(":" + entry.getKey(), sanitizeVariable(entry.getValue()));
    }
    return line;
  }

  private String sanitizeVariable(final String variable) {
    return variable.replace("'", "\\'").replace("\"", "\\\"");
  }
}
