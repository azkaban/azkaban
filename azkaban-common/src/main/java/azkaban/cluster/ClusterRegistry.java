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
package azkaban.cluster;

import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * A registry of all {@link Cluster}s loaded from a local directory of cluster configurations
 * Each instance of {@link Cluster} is created from a directory with the directory
 * name being the cluster id (to ensure uniqueness) and its properties being loaded from
 * the cluster.properties file under that directory.
 *
 * For example, if a local folder with the following layout is configured to load clusters from
 * clusters
 *     cluster-1
 *         cluster.properties
 *     cluster-2
 *         cluster.properties
 *
 *  There would be two clusters in {@link ClusterRegistry}, with their ids being cluster-1 and
 *  cluster-2 respectively.
 */
@Singleton
public class ClusterRegistry {

  private final Map<String, Cluster> clusterInfoMap;

  @Inject
  public ClusterRegistry() {
    this.clusterInfoMap = new HashMap<>();
  }

  public synchronized Cluster getCluster(final String clusterId) {
    return this.clusterInfoMap.getOrDefault(clusterId, Cluster.UNKNOWN);
  }

  public synchronized Cluster addCluster(final String clusterId, final Cluster cluster) {
    return this.clusterInfoMap.put(clusterId, cluster);
  }

  public synchronized boolean isEmpty() {
    return this.clusterInfoMap.isEmpty();
  }
}
