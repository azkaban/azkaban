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

import static azkaban.Constants.FlowProperties.AZKABAN_FLOW_EXEC_ID;

import azkaban.cluster.Cluster;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.log4j.Logger;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class YarnUtils {

  //Yarn resource configuration directory for the cluster where the job is scheduled by the cluster router
  public static final String YARN_CONF_DIRECTORY_PROPERTY = "env.YARN_CONF_DIR";
  public static final String YARN_CONF_FILENAME = "yarn-site.xml";
  public static final String YARN_APP_TIMEOUT_PROPERTY_NAME = "yarn.client.application-client"
      + "-protocol.poll-timeout-ms";
  public static final Integer YARN_APP_TIMEOUT_IN_MILLIONSECONDS = 5000;

  public static final EnumSet<YarnApplicationState> YARN_APPLICATION_ALIVE_STATES = EnumSet.of(
      YarnApplicationState.NEW,
      YarnApplicationState.NEW_SAVING,
      YarnApplicationState.SUBMITTED,
      YarnApplicationState.ACCEPTED,
      YarnApplicationState.RUNNING
  );


  public static Set<String> getAllAliveAppIDsByExecID(final YarnClient yarnClient,
      final String flowExecID, final Logger log)
      throws IOException, YarnException {
    return getAllAliveAppReportsByExecID(yarnClient, flowExecID, log)
        .stream()
        .map(ApplicationReport::getApplicationId)
        .map(ApplicationId::toString).collect(Collectors.toSet());
  }

  /**
   * Use the yarnClient to query the unfinished yarn applications for 1 flow execution
   *
   * @param yarnClient the yarnClient already connects to the cluster
   * @param flowExecID the azkaban flow execution id whose yarn applications needs to be killed
   * @return the set of all to-be-killed (alive) yarn applications' IDs
   * @throws IOException   for RPC issue
   * @throws YarnException for YARN server issue
   */
  public static List<ApplicationReport> getAllAliveAppReportsByExecID(final YarnClient yarnClient,
      final String flowExecID, final Logger log)
      throws IOException, YarnException {

    // format: tagName:tagValue
    Set<String> searchTags = ImmutableSet.of(AZKABAN_FLOW_EXEC_ID + ":" + flowExecID);

    log.info(String.format("Searching for alive yarn application reports with tag %s", searchTags));
    return yarnClient.getApplications(null, YARN_APPLICATION_ALIVE_STATES, searchTags);
  }

  /**
   * Use the yarnClient to query the unfinished yarn applications using a set of flow execution IDs
   * (the union of yarn applications tagged with any of the flow execution IDs)
   */
  public static List<ApplicationReport> getAllAliveAppReportsByExecIDs(final YarnClient yarnClient,
      final Set<Integer> flowExecIDs, final Logger log)
      throws IOException, YarnException {
    if (flowExecIDs.isEmpty()) {
      return Collections.emptyList();
    }

    Set<String> searchTags = flowExecIDs.stream()
        .map(id -> AZKABAN_FLOW_EXEC_ID + ":" + id)
        .collect(Collectors.toSet());
    log.info(String.format("Searching for alive yarn application reports with tags %s",
        searchTags));

    return yarnClient.getApplications(null, YARN_APPLICATION_ALIVE_STATES, searchTags);
  }


  /**
   * Uses YarnClient to kill the jobs one by one, each kill has a timeout
   */
  public static void killAllAppsOnCluster(YarnClient yarnClient, Set<String> applicationIDs,
      Logger log) {
    log.info(String.format("Killing applications: %s", applicationIDs));
    ExecutorService executor = Executors.newSingleThreadExecutor();

    for (final String appId : applicationIDs) {
      Future<?> future = executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            YarnUtils.killAppOnCluster(yarnClient, appId, log);
          } catch (final Throwable t) {
            log.warn("something happened while trying to kill this job: " + appId, t);
          }
        }
      });
      // wait for the kill with a timeout
      try {
        future.get(YARN_APP_TIMEOUT_IN_MILLIONSECONDS, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        log.warn("Timed out killing the yarn app " + appId + ", cancelling the runnable...");
        future.cancel(true);
      } catch (Exception e) {
        log.warn("Exception trying to get the future killing this job: " + appId);
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

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    log.info("killAppOnCluster.getCurrentUser = " + ugi);
    for (Token<?> token : ugi.getCredentials().getAllTokens()) {
      log.info(String.format("killAppOnCluster.getCurrentUser.Token = %s, %s", token.getKind(), token.getService()));
    }
    log.info("killAppOnCluster.getCurrentUser.Token --- end");

    ugi = UserGroupInformation.getLoginUser();
    log.info("killAppOnCluster.getLoginUser = " + ugi);
    for (Token<?> token : ugi.getCredentials().getAllTokens()) {
      log.info(String.format("killAppOnCluster.getLoginUser.Token = %s, %s", token.getKind(), token.getService()));
    }
    log.info("killAppOnCluster.getLoginUser.Token --- end");

    final String[] split = applicationId.split("_");
    final ApplicationId aid = ApplicationId.newInstance(Long.parseLong(split[1]),
        Integer.parseInt(split[2]));
    log.info("start killing application: " + aid);
    yarnClient.killApplication(aid);
    log.info("successfully killed application: " + aid);
  }

  public static void killApplicationAsProxyUser(Cluster cluster,
      ApplicationReport app, final Logger log)
      throws IOException, InterruptedException {
    try {
      UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(
          app.getUser(), UserGroupInformation.getLoginUser());
      proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {

          log.info("proxy as user: " + proxyUser);
          for (Token<?> token : proxyUser.getTokens()) {
            proxyUser.addToken(token);
            log.info(String.format("proxyUser.token = %s, %s, %s ", token.getKind(),
                token.getService(),
                Arrays.toString(token.getIdentifier())));
          }

          YarnClient yarnClient = createYarnClient(cluster.getProperties(), log);
          yarnClient.killApplication(app.getApplicationId());
          return null;
        }
      });
    } catch (Exception e) {
      log.warn("Fail to killApplication as proxy user " + app.getUser(), e);
      throw new RuntimeException("Fail to killApplication as proxy user " + app.getUser(), e);
    }
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

    yarnConf.setLong(YARN_APP_TIMEOUT_PROPERTY_NAME, YARN_APP_TIMEOUT_IN_MILLIONSECONDS);
    final YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(yarnConf);
    yarnClient.start();
    return yarnClient;
  }
}
