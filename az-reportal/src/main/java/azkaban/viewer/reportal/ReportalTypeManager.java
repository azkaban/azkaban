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

package azkaban.viewer.reportal;

import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.JavaProcessJob;
import azkaban.reportal.util.Reportal;
import azkaban.utils.Props;
import java.io.File;
import java.util.Map;

public class ReportalTypeManager {
  public static final String DATA_COLLECTOR_JOB = "ReportalDataCollector";
  public static final String DATA_COLLECTOR_JOB_TYPE = "reportaldatacollector";

  public static void createJobAndFiles(final Reportal reportal, final File jobFile,
      final String jobName, final String queryTitle, final String queryType,
      final String queryScript,
      final String dependentJob, final String userName, final Map<String, String> extras)
      throws Exception {

    // Create props for the job
    final Props propertiesFile = new Props();
    propertiesFile.put("title", queryTitle);

    final ReportalType type = ReportalType.getTypeByName(queryType);

    if (type == null) {
      throw new Exception("Type " + queryType + " is invalid.");
    }

    propertiesFile.put("reportal.title", reportal.title);
    propertiesFile.put("reportal.job.title", jobName);
    propertiesFile.put("reportal.job.query", queryScript);
    propertiesFile.put("user.to.proxy", "${reportal.execution.user}");
    propertiesFile.put("reportal.proxy.user", "${reportal.execution.user}");

    type.buildJobFiles(reportal, propertiesFile, jobFile, jobName, queryScript,
        userName);

    propertiesFile.put(CommonJobProperties.JOB_TYPE, type.getJobTypeName());
    propertiesFile.put(JavaProcessJob.JVM_PARAMS, "-Dreportal.user.name=${reportal.execution.user}"
        + " -Dreportal.execid=${azkaban.flow.execid}");

    // Order dependency
    if (dependentJob != null) {
      propertiesFile.put(CommonJobProperties.DEPENDENCIES, dependentJob);
    }

    if (extras != null) {
      propertiesFile.putAll(extras);
    }

    propertiesFile.storeLocal(jobFile);
  }
}
