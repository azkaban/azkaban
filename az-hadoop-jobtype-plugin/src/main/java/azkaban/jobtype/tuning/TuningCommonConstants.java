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

package azkaban.jobtype.tuning;

/**
 * Tuning related constants.
 */
public class TuningCommonConstants {

  private TuningCommonConstants() {

  }

  /**
   * In case if job is failed because of auto tuning parameters, it will be retried with the best parameters
   * we have seen so far. Maximum number of try by default is 2, which can be configured using parameter
   * tuning.job.retry.count
   */
  public static final String TUNING_JOB_RETRY_COUNT = "tuning.job.retry.count";
  public static final String AUTO_TUNING_JOB_TYPE = "auto.tuning.job.type";
  public static final String OPTIMIZATION_METRIC = "optimization.metric";

  public static final String TUNING_API_END_POINT = "tuning.api.end.point";

  public static final String ALLOWED_MAX_RESOURCE_USAGE_PERCENT = "allowed_max_resource_usage_percent";
  public static final String ALLOWED_MAX_EXECUTION_TIME_PERCENT = "allowed_max_execution_time_percent";

  public static final String AUTO_TUNING_RETRY = "auto.tuning.retry";
  public static final String AUTO_TUNING_ENABLED = "auto.tuning.enabled";

  public static final String PROJECT_NAME_API_PARAM = "projectName";
  public static final String FLOW_DEFINITION_ID_API_PARAM = "flowDefId";
  public static final String JOB_DEFINITION_ID_API_PARAM = "jobDefId";
  public static final String FLOW_EXECUTION_ID_API_PARAM = "flowExecId";
  public static final String JOB_EXECUTION_ID_API_PARAM = "jobExecId";
  public static final String FLOW_DEFINITION_URL_API_PARAM = "flowDefUrl";
  public static final String JOB_DEFINITION_URL_API_PARAM = "jobDefUrl";
  public static final String FLOW_EXECUTION_URL_API_PARAM = "flowExecUrl";
  public static final String JOB_EXECUTION_URL_API_PARAM = "jobExecUrl";
  public static final String JOB_NAME_API_PARAM = "jobName";
  public static final String DEFAULT_PARAM_API_PARAM = "defaultParams";
  public static final String SCHEDULER_API_PARAM = "scheduler";
  public static final String CLIENT_API_PARAM = "client";
  public static final String AUTO_TUNING_JOB_TYPE_API_PARAM = "autoTuningJobType";
  public static final String OPTIMIZATION_METRIC_API_PARAM = "optimizationMetric";
  public static final String USER_NAME_API_PARAM = "userName";
  public static final String IS_RETRY_API_PARAM = "isRetry";
  public static final String SKIP_EXECUTION_FOR_OPTIMIZATION_API_PARAM = "skipExecutionForOptimization";
  public static final String ALLOWED_MAX_EXECUTION_TIME_PERCENT_API_PARAM = "allowedMaxExecutionTimePercent";
  public static final String ALLOWED_MAX_RESOURCE_USAGE_PERCENT_PERCENT_API_PARAM = "allowedMaxResourceUsagePercent";
  public static final String VERSION_API_PARAM = "version";
}
