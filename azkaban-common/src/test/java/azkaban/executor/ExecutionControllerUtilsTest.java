package azkaban.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import azkaban.Constants.ConfigurationKeys;
import azkaban.Constants.FlowParameters;
import azkaban.utils.AuthenticationUtils;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit tests for {@link ExecutionControllerUtils}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AuthenticationUtils.class, ExecutionControllerUtils.class})
public class ExecutionControllerUtilsTest {

  /**
   * Verify for a Spark job that is not routed to any cluster, its job link url is based on Spark
   * History Server URL when the RM job link is invalid.
   */
  @Test
  public void jobLinkUrlBasedOnSparkHistoryServerUrlForUnroutedJobs() throws Exception {
    mockStatic(AuthenticationUtils.class);

    final HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connection.getInputStream()).thenReturn(
        new ByteArrayInputStream("Failed to read the application".getBytes("UTF-8"))
    );
    // mock AuthenticationUtils so that RM job link is no longer valid
    when(AuthenticationUtils.loginAuthenticatedURL(any(URL.class), anyString(), anyString()))
        .thenReturn(connection);

    // create a flow that contains one job that was never routed
    final ExecutableNode node = createExecutableNode("testJob", "spark", null);
    final ExecutableFlow flow = createSingleNodeFlow(node);

    // populate azkaban web server properties
    final Props azkProps = new Props();
    final String resourceManagerUrl =
        "http://localhost:8088/cluster/app/application_${application.id}";
    azkProps.put(ConfigurationKeys.RESOURCE_MANAGER_JOB_URL, resourceManagerUrl);
    final String historyServerUrl =
        "http://localhost:19888/jobhistory/job/job_${application.id}";
    azkProps.put(ConfigurationKeys.HISTORY_SERVER_JOB_URL, historyServerUrl);
    final String sparkHistoryServerUrl =
        "http://localhost:18080/history/application_${application.id}/1/jobs";
    azkProps.put(ConfigurationKeys.SPARK_HISTORY_SERVER_JOB_URL, sparkHistoryServerUrl);
    azkProps.put(ConfigurationKeys.AZKABAN_KEYTAB_PATH, "/fakepath");
    azkProps.put(ConfigurationKeys.AZKABAN_KERBEROS_PRINCIPAL, "azkatest");

    // final ExecutableFlow exFlow, final String jobId, final String applicationId, final Props azkProps)
    final String applicationId = "123456789";
    final String jobLinkUrl = ExecutionControllerUtils.createJobLinkUrl(
        flow, node.getId(), applicationId, azkProps);

    final String expectedJobLinkUrl = sparkHistoryServerUrl.replace(
        ExecutionControllerUtils.OLD_APPLICATION_ID, applicationId);
    Assert.assertEquals(expectedJobLinkUrl, jobLinkUrl);
  }

  /**
   * Verify for a a given job that is not routed to any cluster, a job link url based on Resource
   * Manager URL if returned when the RM job link is still valid.
   */
  @Test
  public void jobLinkUrlBasedOnResourceManagerUrlForUnroutedJobs() throws Exception {
    mockStatic(AuthenticationUtils.class);

    final HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connection.getInputStream()).thenReturn(
        new ByteArrayInputStream("SUCCESS".getBytes("UTF-8"))
    );
    // mock AuthenticationUtils so that RM request to validate job link succeeds
    when(AuthenticationUtils.loginAuthenticatedURL(any(URL.class), anyString(), anyString()))
        .thenReturn(connection);

    // create a flow that contains one node that was never routed, having no any cluster info
    final ExecutableNode node = createExecutableNode("testJob", "spark", null);
    final ExecutableFlow flow = createSingleNodeFlow(node);

    // populate azkaban web server properties
    final Props azkProps = new Props();
    final String resourceManagerUrl =
        "http://localhost:8088/cluster/app/application_${application.id}";
    azkProps.put(ConfigurationKeys.RESOURCE_MANAGER_JOB_URL, resourceManagerUrl);
    final String historyServerUrl =
        "http://localhost:19888/jobhistory/job/job_${application.id}";
    azkProps.put(ConfigurationKeys.HISTORY_SERVER_JOB_URL, historyServerUrl);
    final String sparkHistoryServerUrl =
        "http://localhost:18080/history/application_${application.id}/1/jobs";
    azkProps.put(ConfigurationKeys.SPARK_HISTORY_SERVER_JOB_URL, sparkHistoryServerUrl);
    azkProps.put(ConfigurationKeys.AZKABAN_KEYTAB_PATH, "/fakepath");
    azkProps.put(ConfigurationKeys.AZKABAN_KERBEROS_PRINCIPAL, "azkatest");

    // final ExecutableFlow exFlow, final String jobId, final String applicationId, final Props azkProps)
    final String applicationId = "123456789";
    final String jobLinkUrl = ExecutionControllerUtils.createJobLinkUrl(
        flow, node.getId(), applicationId, azkProps);

    Assert.assertEquals(resourceManagerUrl.replace(ExecutionControllerUtils.OLD_APPLICATION_ID,
        applicationId), jobLinkUrl);
  }

  /**
   * Verify for a given job that is not routed to any cluster, no job link URL is returned when the
   * connection to RM to validate the RM job link fails.
   */
  @Test
  public void noJobLinkUrlForUnroutedJobWhenResourceManagerConnectionFails() throws Exception {
    mockStatic(AuthenticationUtils.class);

    // mock AuthenticationUtils so that RM request to validate job link fails
    when(AuthenticationUtils.loginAuthenticatedURL(any(URL.class), anyString(), anyString()))
        .thenThrow(new Exception("Connection failed"));

    // create a flow that contains one node that was never routed, having no any cluster info
    final ExecutableNode node = createExecutableNode("testJob", "spark", null);
    final ExecutableFlow flow = createSingleNodeFlow(node);

    // populate azkaban web server properties
    final Props azkProps = new Props();
    final String resourceManagerUrl =
        "http://localhost:8088/cluster/app/application_${application.id}";
    azkProps.put(ConfigurationKeys.RESOURCE_MANAGER_JOB_URL, resourceManagerUrl);
    final String historyServerUrl =
        "http://localhost:19888/jobhistory/job/job_${application.id}";
    azkProps.put(ConfigurationKeys.HISTORY_SERVER_JOB_URL, historyServerUrl);
    final String sparkHistoryServerUrl =
        "http://localhost:18080/history/application_${application.id}/1/jobs";
    azkProps.put(ConfigurationKeys.SPARK_HISTORY_SERVER_JOB_URL, sparkHistoryServerUrl);
    azkProps.put(ConfigurationKeys.AZKABAN_KEYTAB_PATH, "/fakepath");
    azkProps.put(ConfigurationKeys.AZKABAN_KERBEROS_PRINCIPAL, "azkatest");

    // final ExecutableFlow exFlow, final String jobId, final String applicationId, final Props azkProps)
    final String applicationId = "123456789";
    final String jobLinkUrl = ExecutionControllerUtils.createJobLinkUrl(
        flow, node.getId(), applicationId, azkProps);

    Assert.assertEquals(null, jobLinkUrl);
  }

  /**
   * Verify for a given Spark job routed to a cluster previously, its job link url is based on Spark
   * History Server URL when the RM job link is invalid.
   */
  @Test
  public void jobLinkUrlBasedOnSparkHistoryServerUrlForRoutedJobs() throws Exception {
    mockStatic(AuthenticationUtils.class);

    final HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connection.getInputStream()).thenReturn(
        new ByteArrayInputStream("Failed to read the application".getBytes("UTF-8"))
    );
    // mock AuthenticationUtils so that RM job link is no longer valid
    when(AuthenticationUtils.loginAuthenticatedURL(any(URL.class), anyString(), anyString()))
        .thenReturn(connection);

    // create a flow that contains one job that was routed to a test cluster
    final String resourceManagerUrl =
        "http://localhost:8088/cluster/app/application_${application.id}";
    final String historyServerUrl =
        "http://localhost:19888/jobhistory/job/job_${application.id}";
    final String sparkHistoryServerUrl =
        "http://localhost:18080/history/application_${application.id}/1/jobs";
    final String hadoopClusterUrl = "http://localhost:8088";
    final ClusterInfo cluster = new ClusterInfo("testCluster", hadoopClusterUrl,
        resourceManagerUrl, historyServerUrl, sparkHistoryServerUrl);
    final ExecutableNode node = createExecutableNode("testJob", "spark", cluster);
    final ExecutableFlow flow = createSingleNodeFlow(node);

    // populate azkaban web server properties
    final Props azkProps = new Props();
    azkProps.put(ConfigurationKeys.AZKABAN_KEYTAB_PATH, "/fakepath");
    azkProps.put(ConfigurationKeys.AZKABAN_KERBEROS_PRINCIPAL, "azkatest");

    // final ExecutableFlow exFlow, final String jobId, final String applicationId, final Props azkProps)
    final String applicationId = "123456789";
    final String jobLinkUrl = ExecutionControllerUtils.createJobLinkUrl(
        flow, node.getId(), applicationId, azkProps);

    final String expectedJobLinkUrl =
        sparkHistoryServerUrl.replace(ExecutionControllerUtils.NEW_APPLICATION_ID, applicationId);
    Assert.assertEquals(expectedJobLinkUrl, jobLinkUrl);
  }

  /**
   * Verify for a given job that is routed to a cluster previously, no job link url is returned if
   * Resource Manager URL, Spark History Server URL or History Server URL, is missing.
   */
  @Test
  public void noJobLinkUrlForRoutedJobsWhenMissingFullClusterInfo() throws Exception {
    mockStatic(AuthenticationUtils.class);

    final HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connection.getInputStream()).thenReturn(
        new ByteArrayInputStream("Failed to read the application".getBytes("UTF-8"))
    );
    // mock AuthenticationUtils so that RM job link is no longer valid
    when(AuthenticationUtils.loginAuthenticatedURL(any(URL.class), anyString(), anyString()))
        .thenReturn(connection);

    final String resourceManagerUrl =
        "http://localhost:8088/cluster/app/application_${application.id}";
    final String sparkHistoryServerUrl =
        "http://localhost:18080/history/application_${application.id}/1/jobs";
    final String hadoopClusterUrl = "http://localhost:8088";
    // create a cluster that is missing History Server URL
    final ClusterInfo cluster = new ClusterInfo("testCluster", hadoopClusterUrl,
        resourceManagerUrl, null, sparkHistoryServerUrl);
    final ExecutableNode node = createExecutableNode("testJob", "spark", cluster);
    // create a flow that contains one job that was routed to a test cluster
    final ExecutableFlow flow = createSingleNodeFlow(node);

    // populate azkaban web server properties
    final Props azkProps = new Props();
    azkProps.put(ConfigurationKeys.AZKABAN_KEYTAB_PATH, "/fakepath");
    azkProps.put(ConfigurationKeys.AZKABAN_KERBEROS_PRINCIPAL, "azkatest");

    // final ExecutableFlow exFlow, final String jobId, final String applicationId, final Props azkProps)
    final String applicationId = "123456789";
    final String jobLinkUrl = ExecutionControllerUtils.createJobLinkUrl(
        flow, node.getId(), applicationId, azkProps);

    Assert.assertNull(jobLinkUrl);
  }

  /**
   * Verify for a a given job that is routed to a cluster previously, a job link url based on
   * Resource Manager URL is returned when the RM job link is still valid.
   */
  @Test
  public void jobLinkUrlBasedOnResourceManagerUrlForRoutedJobs() throws Exception {
    mockStatic(AuthenticationUtils.class);

    final HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connection.getInputStream()).thenReturn(
        new ByteArrayInputStream("SUCCESS".getBytes("UTF-8"))
    );
    // mock AuthenticationUtils so that RM request to validate job link succeeds
    when(AuthenticationUtils.loginAuthenticatedURL(any(URL.class), anyString(), anyString()))
        .thenReturn(connection);

    // create a flow that contains one job that was routed to a test cluster
    final String resourceManagerUrl =
        "http://localhost:8088/cluster/app/application_${application.id}";
    final String historyServerUrl =
        "http://localhost:19888/jobhistory/job/job_${application.id}";
    final String sparkHistoryServerUrl =
        "http://localhost:18080/history/application_${application.id}/1/jobs";
    final String hadoopClusterUrl = "http://localhost:8088";
    final ClusterInfo cluster = new ClusterInfo("testCluster", hadoopClusterUrl,
        resourceManagerUrl, historyServerUrl, sparkHistoryServerUrl);
    final ExecutableNode node = createExecutableNode("testJob", "spark", cluster);
    final ExecutableFlow flow = createSingleNodeFlow(node);

    // populate azkaban web server properties
    final Props azkProps = new Props();
    azkProps.put(ConfigurationKeys.AZKABAN_KEYTAB_PATH, "/fakepath");
    azkProps.put(ConfigurationKeys.AZKABAN_KERBEROS_PRINCIPAL, "azkatest");

    // final ExecutableFlow exFlow, final String jobId, final String applicationId, final Props azkProps)
    final String applicationId = "123456789";
    final String jobLinkUrl = ExecutionControllerUtils.createJobLinkUrl(
        flow, node.getId(), applicationId, azkProps);

    final String expectedJobLinkUrl = resourceManagerUrl.replace(
        ExecutionControllerUtils.NEW_APPLICATION_ID, applicationId);
    Assert.assertEquals(expectedJobLinkUrl, jobLinkUrl);
  }

  /**
   * Verify for a given job that is routed to a cluster previously, no job link URL is returned when
   * the connection to RM to validate the RM job link fails.
   */
  @Test
  public void noJobLinkUrlForRoutedJobWhenResourceManagerConnectionFails() throws Exception {
    mockStatic(AuthenticationUtils.class);

    // mock AuthenticationUtils so that RM request to validate job link fails
    when(AuthenticationUtils.loginAuthenticatedURL(any(URL.class), anyString(), anyString()))
        .thenThrow(new Exception("RM Connection failed"));

    // create a flow that contains one job that was routed to a test cluster
    final String resourceManagerUrl =
        "http://localhost:8088/cluster/app/application_${application.id}";
    final String historyServerUrl =
        "http://localhost:19888/jobhistory/job/job_${application.id}";
    final String sparkHistoryServerUrl =
        "http://localhost:18080/history/application_${application.id}/1/jobs";
    final String hadoopClusterUrl = "http://localhost:8088";
    final ClusterInfo cluster = new ClusterInfo("testCluster", hadoopClusterUrl,
        resourceManagerUrl, historyServerUrl, sparkHistoryServerUrl);
    final ExecutableNode node = createExecutableNode("testJob", "spark", cluster);
    final ExecutableFlow flow = createSingleNodeFlow(node);

    // populate azkaban web server properties
    final Props azkProps = new Props();
    azkProps.put(ConfigurationKeys.AZKABAN_KEYTAB_PATH, "/fakepath");
    azkProps.put(ConfigurationKeys.AZKABAN_KERBEROS_PRINCIPAL, "azkatest");

    // final ExecutableFlow exFlow, final String jobId, final String applicationId, final Props azkProps)
    final String applicationId = "123456789";
    final String jobLinkUrl = ExecutionControllerUtils.createJobLinkUrl(
        flow, node.getId(), applicationId, azkProps);

    Assert.assertNull(jobLinkUrl);
  }

  private ExecutableNode createExecutableNode(final String id, final String type,
      final ClusterInfo clusterInfo) {
    final ExecutableNode node = mock(ExecutableNode.class);
    when(node.getType()).thenReturn(type);
    when(node.getId()).thenReturn(id);
    when(node.getClusterInfo()).thenReturn(clusterInfo);
    return node;
  }

  private ExecutableFlow createSingleNodeFlow(final ExecutableNode node) {
    final ExecutableFlow flow = mock(ExecutableFlow.class);
    when(flow.getExecutableNodePath(anyString())).thenReturn(node);
    return flow;
  }

  @Test
  public void testGetFlowToRestartSuccess_EXECUTION_STOPPED() {
    final ExecutableNode node = createExecutableNode("testJob", "spark", null);
    ExecutableFlow testFlow = createSingleNodeFlow(node);

    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED,FAILED",
        FlowParameters.FLOW_PARAM_MAX_RETRIES, "2"
    ));
    when(testFlow.getExecutionOptions()).thenReturn(options);

    ExecutableFlow flowToRestart = ExecutionControllerUtils.getFlowToRestart(testFlow,
        Status.EXECUTION_STOPPED);
    assertNotNull(flowToRestart);
    assertEquals(1, flowToRestart.getSystemDefinedRetryCount());
  }


  @Test
  public void testGetFlowToRestartNoExecutionOptions() {
    final ExecutableNode node = createExecutableNode("testJob", "spark", null);
    ExecutableFlow testFlow = createSingleNodeFlow(node);
    when(testFlow.getExecutionOptions()).thenReturn(null);

    ExecutableFlow flowToRestart = ExecutionControllerUtils.getFlowToRestart(testFlow,
        Status.PREPARING);
    assertNull(flowToRestart);
  }

  @Test
  public void testGetFlowToRestartNoSystemRetriedExceed() {
    final ExecutableNode node = createExecutableNode("testJob", "spark", null);
    ExecutableFlow testFlow = createSingleNodeFlow(node);
    when(testFlow.getSystemDefinedRetryCount()).thenReturn(1);
    ExecutionOptions options = new ExecutionOptions();
    when(testFlow.getExecutionOptions()).thenReturn(options);

    ExecutableFlow flowToRestart = ExecutionControllerUtils.getFlowToRestart(testFlow,
        Status.PREPARING);
    assertNull(flowToRestart);
  }

  @Test
  public void testGetFlowToRestart_NoOperation_NotIncludedStatus_KILLED() {
    final ExecutableNode node = createExecutableNode("testJob", "spark", null);
    ExecutableFlow testFlow = createSingleNodeFlow(node);

    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED,FAILED",
        FlowParameters.FLOW_PARAM_MAX_RETRIES, "2"
    ));
    when(testFlow.getExecutionOptions()).thenReturn(options);

    ExecutableFlow flowToRestart = ExecutionControllerUtils.getFlowToRestart(testFlow,
        Status.KILLED);
    assertNull(flowToRestart);
  }

  @Test
  public void testGetFlowToRestartSuccess_PREPARING() {
    final ExecutableNode node = createExecutableNode("testJob", "spark", null);
    ExecutableFlow testFlow = createSingleNodeFlow(node);

    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED,FAILED",
        FlowParameters.FLOW_PARAM_MAX_RETRIES, "2"
    ));
    when(testFlow.getExecutionOptions()).thenReturn(options);

    ExecutableFlow flowToRestart = ExecutionControllerUtils.getFlowToRestart(testFlow,
        Status.PREPARING);
    assertNotNull(flowToRestart);
    assertEquals(1, flowToRestart.getUserDefinedRetryCount());
    assertEquals("2",
        flowToRestart
            .getExecutionOptions()
            .getFlowParameters()
            .get(FlowParameters.FLOW_PARAM_MAX_RETRIES));
  }

  @Test
  public void testGetFlowToRestartFAIL_ALREADY_MAX_RETRY() throws RuntimeException {
    final ExecutableNode node = createExecutableNode("testJob", "spark", null);
    ExecutableFlow testFlow = createSingleNodeFlow(node);

    ExecutionOptions options = new ExecutionOptions();
    options.addAllFlowParameters(ImmutableMap.of(
        FlowParameters.FLOW_PARAM_ALLOW_RESTART_ON_STATUS, "EXECUTION_STOPPED,FAILED",
        FlowParameters.FLOW_PARAM_MAX_RETRIES, "0"
    ));
    when(testFlow.getExecutionOptions()).thenReturn(options);

    ExecutableFlow flowToRestart = ExecutionControllerUtils.getFlowToRestart(testFlow,
        Status.EXECUTION_STOPPED);
    assertNull(flowToRestart);
  }
}
