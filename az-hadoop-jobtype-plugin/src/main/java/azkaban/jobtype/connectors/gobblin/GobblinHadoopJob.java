/*
 * Copyright 2014-2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.jobtype.connectors.gobblin;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;

import azkaban.flow.CommonJobProperties;
import azkaban.jobtype.HadoopJavaJob;
import azkaban.jobtype.connectors.gobblin.helper.HdfsToMySqlValidator;
import azkaban.jobtype.connectors.gobblin.helper.IPropertiesValidator;
import azkaban.jobtype.connectors.gobblin.helper.MySqlToHdfsValidator;
import azkaban.utils.Props;

/**
 * Integration Azkaban with Gobblin. It prepares job properties for Gobblin and utilizes HadoopJavaJob to kick off the job
 */
public class GobblinHadoopJob extends HadoopJavaJob {
  private static final String GOBBLIN_PRESET_COMMON_PROPERTIES_FILE_NAME = "common.properties";
  private static final String GOBBLIN_QUERY_KEY = "source.querybased.query";
  private static volatile Map<GobblinPresets, Properties> gobblinPresets;


  public GobblinHadoopJob(String jobid, Props sysProps, Props jobProps, Logger log) {
    super(jobid, sysProps, jobProps, log);
    initializePresets();

    jobProps.put("job.class", "gobblin.azkaban.AzkabanJobLauncher");
    jobProps.put("job.name", jobProps.get(CommonJobProperties.JOB_ID));
    jobProps.put("launcher.type", "MAPREDUCE"); //Azkaban only supports MR mode
    jobProps.put("fs.uri", sysProps.get("fs.uri")); //Azkaban should only support HDFS

    //If gobblin jars are in HDFS pass HDFS path to Gobblin, otherwise pass local file system path.
    if (sysProps.containsKey(GobblinConstants.GOBBLIN_HDFS_JOB_JARS_KEY)) {
      jobProps.put(GobblinConstants.GOBBLIN_HDFS_JOB_JARS_KEY, sysProps.getString(GobblinConstants.GOBBLIN_HDFS_JOB_JARS_KEY));
    } else {
      jobProps.put(GobblinConstants.GOBBLIN_JOB_JARS_KEY, sysProps.get("jobtype.classpath"));
    }

    loadPreset();
    transformProperties();
    getLog().info("Job properties for Gobblin: " + printableJobProperties(jobProps));
  }

  /**
   * Factory method that provides IPropertiesValidator based on preset in runtime.
   * Using factory method pattern as it is expected to grow.
   * @param preset
   * @return IPropertiesValidator
   */
  private static IPropertiesValidator getValidator(GobblinPresets preset) {
    Objects.requireNonNull(preset);
    switch (preset) {
      case MYSQL_TO_HDFS:
        return new MySqlToHdfsValidator();
      case HDFS_TO_MYSQL:
        return new HdfsToMySqlValidator();
      default:
        throw new UnsupportedOperationException("Preset " + preset + " is not supported");
    }
  }

  /**
   * Print the job properties except property key contains "pass" and "word".
   * @param jobProps
   */
  @VisibleForTesting
  Map<String, String> printableJobProperties(Props jobProps) {
    Predicate<String> keyPredicate = new Predicate<String>() {

      @Override
      public boolean apply(String key) {
        if (StringUtils.isEmpty(key)) {
          return true;
        }
        key = key.toLowerCase();
        return !(key.contains("pass") && key.contains("word"));
      }

    };
    return Maps.filterKeys(jobProps.getFlattened(), keyPredicate);
  }

  /**
   * Initializes presets and cache it into preset map. As presets do not change while server is up,
   * this initialization happens only once per JVM.
   */
  private void initializePresets() {
    if (gobblinPresets == null) {
      synchronized (GobblinHadoopJob.class) {
        if (gobblinPresets == null) {
          gobblinPresets = Maps.newHashMap();
          String gobblinPresetDirName = sysProps.getString(GobblinConstants.GOBBLIN_PRESET_DIR_KEY);
          File gobblinPresetDir = new File(gobblinPresetDirName);
          File[] presetFiles = gobblinPresetDir.listFiles();
          if (presetFiles == null) {
            return;
          }

          File commonPropertiesFile = new File(gobblinPresetDir, GOBBLIN_PRESET_COMMON_PROPERTIES_FILE_NAME);
          if (!commonPropertiesFile.exists()) {
            throw new IllegalStateException("Gobbline preset common properties file is missing "
                + commonPropertiesFile.getAbsolutePath());
          }

          for (File f : presetFiles) {
            if (GOBBLIN_PRESET_COMMON_PROPERTIES_FILE_NAME.equals(f.getName())) { //Don't load common one itself.
              continue;
            }

            if (f.isFile()) {
              Properties prop = new Properties();
              try (InputStream commonIs = new BufferedInputStream(new FileInputStream(commonPropertiesFile));
                  InputStream presetIs = new BufferedInputStream(new FileInputStream(f))) {
                prop.load(commonIs);
                prop.load(presetIs);

                String presetName = f.getName().substring(0, f.getName().lastIndexOf('.')); //remove extension from the file name
                gobblinPresets.put(GobblinPresets.fromName(presetName), prop);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          }
        }
      }
    }
  }

  /**
   * If input parameter has preset value, it will load set of properties into job property for the Gobblin job.
   * Also, if user wants to validates the job properties(enabled by default), it will validate it based on the preset where
   * preset is basically used as a proxy to the use case.
   */
  private void loadPreset() {
    String presetName = jobProps.get(GobblinConstants.GOBBLIN_PRESET_KEY);
    if (presetName == null) {
      return;
    }

    GobblinPresets preset = GobblinPresets.fromName(presetName);
    Properties presetProperties = gobblinPresets.get(preset);
    if (presetProperties == null) {
      throw new IllegalArgumentException("Preset " + presetName + " is not supported. Supported presets: "
          + gobblinPresets.keySet());
    }

    getLog().info("Loading preset " + presetName + " : " + presetProperties);
    Map<String, String> skipped = Maps.newHashMap();
    for (String key : presetProperties.stringPropertyNames()) {
      if (jobProps.containsKey(key)) {
        skipped.put(key, presetProperties.getProperty(key));
        continue;
      }
      jobProps.put(key, presetProperties.getProperty(key));
    }
    getLog().info("Loaded preset " + presetName);
    if (!skipped.isEmpty()) {
      getLog().info("Skipped some properties from preset as already exists in job properties. Skipped: " + skipped);
    }

    if (jobProps.getBoolean(GobblinConstants.GOBBLIN_PROPERTIES_HELPER_ENABLED_KEY, true)) {
      getValidator(preset).validate(jobProps);
    }
  }

  /**
   * Transform property to make it work for Gobblin.
   *
   * e.g: Gobblin fails when there's semicolon in SQL query as it just appends " and 1=1;" into the query,
   * making the syntax incorrect and fails. As having semicolon is a correct syntax, instead of expecting user to remove it,
   * Azkaban will remove it for user to make it work with Gobblin.
   */
  private void transformProperties() {
    //Gobblin does not accept the SQL query ends with semi-colon
    String query = jobProps.getString(GOBBLIN_QUERY_KEY, null);
    if(query == null) {
      return;
    }

    query = query.trim();
    int idx = -1;
    if ((idx = query.indexOf(';')) >= 0) {
      if(idx < query.length() - 1) {
        //Query string has been already trimmed and if index is not end of the query String,
        //it means there's more than one statement.
        throw new IllegalArgumentException(GOBBLIN_QUERY_KEY + " should consist of one SELECT statement. " + query);
      }
      query = query.substring(0, idx);
      jobProps.put(GOBBLIN_QUERY_KEY, query);
    }
  }
}
