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

package azkaban.executor;

public interface ConnectorParams {
  public static final String EXECUTOR_ID_PARAM = "executorId";
  public static final String ACTION_PARAM = "action";
  public static final String EXECID_PARAM = "execid";
  public static final String SHAREDTOKEN_PARAM = "token";
  public static final String USER_PARAM = "user";

  public static final String UPDATE_ACTION = "update";
  public static final String STATUS_ACTION = "status";
  public static final String EXECUTE_ACTION = "execute";
  public static final String CANCEL_ACTION = "cancel";
  public static final String PAUSE_ACTION = "pause";
  public static final String RESUME_ACTION = "resume";
  public static final String PING_ACTION = "ping";
  public static final String LOG_ACTION = "log";
  public static final String ATTACHMENTS_ACTION = "attachments";
  public static final String METADATA_ACTION = "metadata";
  public static final String RELOAD_JOBTYPE_PLUGINS_ACTION = "reloadJobTypePlugins";

  public static final String MODIFY_EXECUTION_ACTION = "modifyExecution";
  public static final String MODIFY_EXECUTION_ACTION_TYPE = "modifyType";
  public static final String MODIFY_RETRY_FAILURES = "retryFailures";
  public static final String MODIFY_RETRY_JOBS = "retryJobs";
  public static final String MODIFY_CANCEL_JOBS = "cancelJobs";
  public static final String MODIFY_DISABLE_JOBS = "skipJobs";
  public static final String MODIFY_ENABLE_JOBS = "enableJobs";
  public static final String MODIFY_PAUSE_JOBS = "pauseJobs";
  public static final String MODIFY_RESUME_JOBS = "resumeJobs";
  public static final String MODIFY_JOBS_LIST = "jobIds";

  public static final String START_PARAM = "start";
  public static final String END_PARAM = "end";
  public static final String STATUS_PARAM = "status";
  public static final String NODES_PARAM = "nodes";
  public static final String EXECPATH_PARAM = "execpath";

  public static final String RESPONSE_NOTFOUND = "notfound";
  public static final String RESPONSE_ERROR = "error";
  public static final String RESPONSE_SUCCESS = "success";
  public static final String RESPONSE_ALIVE = "alive";
  public static final String RESPONSE_UPDATETIME = "lasttime";
  public static final String RESPONSE_UPDATED_FLOWS = "updated";

  public static final int NODE_NAME_INDEX = 0;
  public static final int NODE_STATUS_INDEX = 1;
  public static final int NODE_START_INDEX = 2;
  public static final int NODE_END_INDEX = 3;

  public static final String UPDATE_TIME_LIST_PARAM = "updatetime";
  public static final String EXEC_ID_LIST_PARAM = "executionId";

  public static final String FORCED_FAILED_MARKER = ".failed";

  public static final String UPDATE_MAP_EXEC_ID = "executionId";
  public static final String UPDATE_MAP_JOBID = "jobId";
  public static final String UPDATE_MAP_UPDATE_TIME = "updateTime";
  public static final String UPDATE_MAP_STATUS = "status";
  public static final String UPDATE_MAP_START_TIME = "startTime";
  public static final String UPDATE_MAP_END_TIME = "endTime";
  public static final String UPDATE_MAP_NODES = "nodes";

  public static final String JMX_GET_MBEANS = "getMBeans";
  public static final String JMX_GET_MBEAN_INFO = "getMBeanInfo";
  public static final String JMX_GET_MBEAN_ATTRIBUTE = "getAttribute";
  public static final String JMX_GET_ALL_MBEAN_ATTRIBUTES = "getAllMBeanAttributes";
  public static final String JMX_ATTRIBUTE = "attribute";
  public static final String JMX_MBEAN = "mBean";

  public static final String JMX_GET_ALL_EXECUTOR_ATTRIBUTES = "getAllExecutorAttributes";
  public static final String JMX_HOSTPORT = "hostPort";

  public static final String STATS_GET_ALLMETRICSNAME = "getAllMetricNames";
  public static final String STATS_GET_METRICHISTORY = "getMetricHistory";
  public static final String STATS_SET_REPORTINGINTERVAL = "changeMetricInterval";
  public static final String STATS_SET_CLEANINGINTERVAL = "changeCleaningInterval";
  public static final String STATS_SET_MAXREPORTERPOINTS = "changeEmitterPoints";
  public static final String STATS_SET_ENABLEMETRICS = "enableMetrics";
  public static final String STATS_SET_DISABLEMETRICS = "disableMetrics";
  public static final String STATS_MAP_METRICNAMEPARAM = "metricName";

  /**
   * useStats param is used to filter datapoints on /stats graph by using standard deviation and means
   * By default, we consider only top/bottom 5% datapoints
   */

  public static final String STATS_MAP_METRICRETRIEVALMODE = "useStats";
  public static final String STATS_MAP_STARTDATE = "from";
  public static final String STATS_MAP_ENDDATE = "to";
  public static final String STATS_MAP_REPORTINGINTERVAL = "interval";
  public static final String STATS_MAP_CLEANINGINTERVAL = "interval";
  public static final String STATS_MAP_EMITTERNUMINSTANCES = "numInstances";


}
