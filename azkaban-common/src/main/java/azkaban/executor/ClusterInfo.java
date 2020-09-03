/*
 * Copyright 2020 LinkedIn Corp.
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

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.flow.CommonJobProperties;
import azkaban.utils.TypedMapWrapper;
import java.util.HashMap;
import java.util.Map;

/**
 * The information of a cluster that a job is routed to. NOTE all the urls except for
 * the  Hadoop cluster URL are expected to * have a <application.id> as an application
 * id placeholder for Azkaban Web Server to fill.
 */
public class ClusterInfo {

  public final String resourceManagerURL;
  public final String historyServerURL;
  public final String sparkHistoryServerURL;
  public final String clusterId;
  public final String hadoopClusterURL;

  public ClusterInfo(final String clusterId, final String hadoopClusterURL,
      final String resourceManagerURL,
      final String historyServerURL, final String sparkHistoryServerURL) {
    this.clusterId = clusterId;
    this.hadoopClusterURL = hadoopClusterURL;
    this.resourceManagerURL = resourceManagerURL;
    this.historyServerURL = historyServerURL;
    this.sparkHistoryServerURL = sparkHistoryServerURL;
  }

  public static ClusterInfo fromObject(final Object obj) {
    final Map<String, Object> map = (Map<String, Object>) obj;
    final TypedMapWrapper<String, Object> wrapper = new TypedMapWrapper<>(map);
    final String clusterId = wrapper.getString(CommonJobProperties.TARGET_CLUSTER_ID);
    final String clusterURL = wrapper.getString(Constants.ConfigurationKeys.HADOOP_CLUSTER_URL);
    final String rmURL = wrapper.getString(Constants.ConfigurationKeys.RESOURCE_MANAGER_JOB_URL);
    final String hsURL = wrapper.getString(Constants.ConfigurationKeys.HISTORY_SERVER_JOB_URL);
    final String shsURL = wrapper
        .getString(Constants.ConfigurationKeys.SPARK_HISTORY_SERVER_JOB_URL);
    return new ClusterInfo(clusterId, clusterURL, rmURL, hsURL, shsURL);
  }

  public static Map<String, Object> toObject(final ClusterInfo cluster) {
    final HashMap<String, Object> map = new HashMap<>();
    map.put(CommonJobProperties.TARGET_CLUSTER_ID, cluster.clusterId);
    map.put(ConfigurationKeys.HADOOP_CLUSTER_URL, cluster.hadoopClusterURL);
    map.put(Constants.ConfigurationKeys.RESOURCE_MANAGER_JOB_URL, cluster.resourceManagerURL);
    map.put(Constants.ConfigurationKeys.HISTORY_SERVER_JOB_URL, cluster.historyServerURL);
    map.put(Constants.ConfigurationKeys.SPARK_HISTORY_SERVER_JOB_URL,
        cluster.sparkHistoryServerURL);
    return map;
  }
}
