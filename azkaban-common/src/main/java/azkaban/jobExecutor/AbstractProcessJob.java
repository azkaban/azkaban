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

import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.commons.fileupload.util.Streams;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * A revised process-based job
 */
public abstract class AbstractProcessJob extends AbstractJob {

  public static final String ENV_PREFIX = "env.";
  public static final String ENV_PREFIX_UCASE = "ENV.";
  public static final String WORKING_DIR = "working.dir";
  public static final String JOB_PROP_ENV = "JOB_PROP_FILE";
  public static final String JOB_NAME_ENV = "JOB_NAME";
  public static final String JOB_OUTPUT_PROP_FILE = "JOB_OUTPUT_PROP_FILE";
  private static final String SENSITIVE_JOB_PROP_NAME_SUFFIX = "_X";
  private static final String SENSITIVE_JOB_PROP_VALUE_PLACEHOLDER = "[MASKED]";
  private static final String JOB_DUMP_PROPERTIES_IN_LOG = "job.dump.properties";
  protected final String _jobPath;
  private final Logger log;
  protected volatile Props jobProps;
  protected volatile Props sysProps;

  protected String _cwd;

  private volatile Props generatedProperties;

  protected AbstractProcessJob(final String jobid, final Props sysProps,
      final Props jobProps, final Logger log) {
    super(jobid, log);

    this.jobProps = jobProps;
    this.sysProps = sysProps;
    this._cwd = getWorkingDirectory();
    this._jobPath = this._cwd;

    this.log = log;
  }

  public File createOutputPropsFile(final String id, final String workingDir) {
    this.info("cwd=" + workingDir);

    final File directory = new File(workingDir);
    File tempFile = null;
    try {
      tempFile = File.createTempFile(id + "_output_", "_tmp", directory);
    } catch (final IOException e) {
      this.error("Failed to create temp output property file :", e);
      throw new RuntimeException("Failed to create temp output property file ",
          e);
    }
    return tempFile;
  }

  @Override
  public Props getJobProps() {
    return this.jobProps;
  }

  public Props getSysProps() {
    return this.sysProps;
  }

  public String getJobPath() {
    return this._jobPath;
  }

  protected void resolveProps() {
    this.jobProps = PropsUtils.resolveProps(this.jobProps);
  }

  /**
   * prints the current Job props to the Job log.
   */
  protected void logJobProperties() {
    if (this.jobProps != null &&
        this.jobProps.getBoolean(JOB_DUMP_PROPERTIES_IN_LOG, false)) {
      try {
        final Map<String, String> flattenedProps = this.jobProps.getFlattened();
        this.info("******   Job properties   ******");
        this.info(String.format("- Note : value is masked if property name ends with '%s'.",
            SENSITIVE_JOB_PROP_NAME_SUFFIX));
        for (final Map.Entry<String, String> entry : flattenedProps.entrySet()) {
          final String key = entry.getKey();
          final String value = key.endsWith(SENSITIVE_JOB_PROP_NAME_SUFFIX) ?
              SENSITIVE_JOB_PROP_VALUE_PLACEHOLDER :
              entry.getValue();
          this.info(String.format("%s=%s", key, value));
        }
        this.info("****** End Job properties  ******");
      } catch (final Exception ex) {
        this.log.error("failed to log job properties ", ex);
      }
    }
  }

  @Override
  public Props getJobGeneratedProperties() {
    return this.generatedProperties;
  }

  /**
   * initialize temporary and final property file
   *
   * @return {tmpPropFile, outputPropFile}
   */
  public File[] initPropsFiles() {
    // Create properties file with additionally all input generated properties.
    final File[] files = new File[2];
    files[0] = createFlattenedPropsFile(this._cwd);

    this.jobProps.put(ENV_PREFIX + JOB_PROP_ENV, files[0].getAbsolutePath());
    this.jobProps.put(ENV_PREFIX + JOB_NAME_ENV, getId());

    files[1] = this.createOutputPropsFile(getId(), this._cwd);
    this.jobProps.put(ENV_PREFIX + JOB_OUTPUT_PROP_FILE, files[1].getAbsolutePath());
    return files;
  }

  public String getCwd() {
    return this._cwd;
  }

  public Map<String, String> getEnvironmentVariables() {
    final Props props = getJobProps();
    final Map<String, String> envMap = props.getMapByPrefix(ENV_PREFIX);
    envMap.putAll(props.getMapByPrefix(ENV_PREFIX_UCASE));
    return envMap;
  }

  public String getWorkingDirectory() {
    final String workingDir = getJobProps().getString(WORKING_DIR, this._jobPath);
    if (workingDir == null) {
      return "";
    }

    return workingDir;
  }

  public Props loadOutputFileProps(final File outputPropertiesFile) {
    InputStream reader = null;
    try {
      this.info("output properties file=" + outputPropertiesFile.getAbsolutePath());
      reader =
          new BufferedInputStream(new FileInputStream(outputPropertiesFile));

      final Props outputProps = new Props();
      final String content = Streams.asString(reader).trim();

      if (!content.isEmpty()) {
        final Map<String, Object> propMap =
            (Map<String, Object>) JSONUtils.parseJSONFromString(content);

        for (final Map.Entry<String, Object> entry : propMap.entrySet()) {
          outputProps.put(entry.getKey(), entry.getValue().toString());
        }
      }
      return outputProps;
    } catch (final FileNotFoundException e) {
      this.log.info(String.format("File[%s] wasn't found, returning empty props.",
          outputPropertiesFile));
      return new Props();
    } catch (final Exception e) {
      this.log.error(
          "Exception thrown when trying to load output file props.  Returning empty Props instead of failing.  Is this really the best thing to do?",
          e);
      return new Props();
    } finally {
      IOUtils.closeQuietly(reader);
    }
  }

  public File createFlattenedPropsFile(final String workingDir) {
    final File directory = new File(workingDir);
    File tempFile = null;
    try {
      // The temp file prefix must be at least 3 characters.
      tempFile = File.createTempFile(getId() + "_props_", "_tmp", directory);
      this.jobProps.storeFlattened(tempFile);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to create temp property file ", e);
    }

    return tempFile;
  }

  public void generateProperties(final File outputFile) {
    this.generatedProperties = loadOutputFileProps(outputFile);
  }

}
