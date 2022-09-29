/*
 * Copyright 2022 LinkedIn Corp.
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
package azkaban.utils;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class YarnUtils {

  //Yarn resource configuration directory for the cluster where the job is scheduled by the cluster router
  private static final String YARN_CONF_DIRECTORY_PROPERTY = "env.YARN_CONF_DIR";
  private static final String YARN_CONF_FILENAME = "yarn-site.xml";

  /**
   * Uses YarnClient to kill the jobs one by one
   */
  public static void killAllAppsOnCluster(YarnClient yarnClient, Set<String> applicationIDs,
      Logger log) {
    log.info(String.format("Killing applications: %s", applicationIDs));

    for (final String appId : applicationIDs) {
      try {
        YarnUtils.killAppOnCluster(yarnClient, appId, log);
      } catch (final Throwable t) {
        log.warn("something happened while trying to kill this job: " + appId, t);
      }
    }
  }

  /**
   * <pre>
   * Uses YarnClient to kill the job on the Hadoop Yarn Cluster.
   * Using JobClient only works partially:
   *   If yarn container has started but spark job haven't, it will kill
   *   If spark job has started, the cancel will hang until the spark job is complete
   *   If the spark job is complete, it will return immediately, with a job not found on job tracker
   * </pre>
   */
  public static void killAppOnCluster(final YarnClient yarnClient, final String applicationId,
      final Logger log) throws YarnException, IOException {

    final String[] split = applicationId.split("_");
    final ApplicationId aid = ApplicationId.newInstance(Long.parseLong(split[1]),
        Integer.parseInt(split[2]));
    log.info("start killing application: " + aid);
    yarnClient.killApplication(aid);
    log.info("successfully killed application: " + aid);
  }

  /**
   * Create, initialize and start a YarnClient connecting to the Yarn Cluster (resource manager),
   * using the resources passed in with props.
   *
   * @param props the properties to create a YarnClient, the path to the "yarn-site.xml" to be used
   * @param log
   */
  public static YarnClient createYarnClient(Props props, Logger log) {
    final YarnConfiguration yarnConf = new YarnConfiguration();
    if (props.containsKey(YARN_CONF_DIRECTORY_PROPERTY)) {
      log.info("Job yarn conf dir: " + props.get(YARN_CONF_DIRECTORY_PROPERTY));
      yarnConf.addResource(
          new Path(props.get(YARN_CONF_DIRECTORY_PROPERTY) + "/" + YARN_CONF_FILENAME));
    }
    final YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(yarnConf);
    yarnClient.start();
    return yarnClient;
  }
}
