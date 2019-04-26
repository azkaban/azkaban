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

/**
 * Set of Gobblin constants
 */
public interface GobblinConstants {

  String GOBBLIN_PRESET_DIR_KEY = "gobblin.config.preset.dir"; //Directory where preset file lies.
  String GOBBLIN_PRESET_KEY = "gobblin.config_preset"; //Name of Gobblin preset
  String GOBBLIN_WORK_DIRECTORY_KEY = "gobblin.work_dir"; //Gobblin needs working directory. This will be a HDFS directory.
  String GOBBLIN_PROPERTIES_HELPER_ENABLED_KEY = "gobblin.properties_helper_enabled"; //Validates Gobblin job properties if enabled.
  String GOBBLIN_HDFS_JOB_JARS_KEY = "job.hdfs.jars";
  String GOBBLIN_JOB_JARS_KEY = "job.jars";
}
