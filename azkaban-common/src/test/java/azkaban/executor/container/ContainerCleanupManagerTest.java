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
package azkaban.executor.container;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import azkaban.Constants;
import azkaban.cluster.Cluster;
import azkaban.cluster.ClusterRouter;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionControllerUtils;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.OnContainerizedExecutionEventListener;
import azkaban.executor.Status;
import azkaban.metrics.DummyContainerizationMetricsImpl;
import azkaban.utils.Props;
import azkaban.utils.YarnUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({YarnUtils.class})
public class ContainerCleanupManagerTest {

  private Props props;
  private ExecutorLoader executorLoader;
  private ContainerizedImpl containerImpl;
  private ContainerizedDispatchManager containerizedDispatchManager;
  private ContainerCleanupManager cleaner;
  private DummyContainerizationMetricsImpl metrics = new DummyContainerizationMetricsImpl();
  private ClusterRouter clusterRouter;

  @Before
  public void setup() throws Exception {
    this.props = new Props();
    this.executorLoader = mock(ExecutorLoader.class);
    this.containerImpl = mock(ContainerizedImpl.class);
    this.clusterRouter = mock(ClusterRouter.class);
    when(this.clusterRouter.getAllClusters()).thenReturn(ImmutableMap.of());
    this.containerizedDispatchManager = mock(ContainerizedDispatchManager.class);
    this.cleaner = new ContainerCleanupManager(this.props, this.executorLoader,
        this.clusterRouter, this.containerImpl, this.containerizedDispatchManager, this.metrics);
  }

  @Test
  public void testEmptyStaleExecutions() throws Exception {
    // List of stale flows is empty
    when(this.executorLoader.fetchStaleFlowsForStatus(any(), any())).thenReturn(new ArrayList<>());
    this.cleaner.cleanUpStaleFlows();
    verify(this.executorLoader).fetchStaleFlowsForStatus(Status.DISPATCHING,
        this.cleaner.getValidityMap());
    verify(this.executorLoader)
        .fetchStaleFlowsForStatus(Status.PREPARING, this.cleaner.getValidityMap());
    verify(this.executorLoader)
        .fetchStaleFlowsForStatus(Status.RUNNING, this.cleaner.getValidityMap());
    verify(this.executorLoader)
        .fetchStaleFlowsForStatus(Status.PAUSED, this.cleaner.getValidityMap());
    verify(this.executorLoader)
        .fetchStaleFlowsForStatus(Status.KILLING, this.cleaner.getValidityMap());
    verify(this.executorLoader)
        .fetchStaleFlowsForStatus(Status.FAILED_FINISHING, this.cleaner.getValidityMap());
    verifyZeroInteractions(this.containerImpl);
  }

  @Test
  public void testExceptionInFetchingExecutions() throws Exception {
    // Mock an exception while fetching stale flows.
    doThrow(new RuntimeException("mock runtime exception"))
        .when(this.executorLoader).fetchStaleFlowsForStatus(any(), any());
    // Verifies that exception is consumed, otherwise this test will fail with exception.
    this.cleaner.cleanUpStaleFlows();
    // Additionally verify  no invocations for container deletion should take place
    verifyZeroInteractions(this.containerImpl);
  }

  @Ignore
  @Test
  public void testCleanUpPreparingFlows() throws Exception {
    ArrayList<ExecutableFlow> executableFlows = new ArrayList<>();
    ExecutableFlow flow = new ExecutableFlow();
    flow.setExecutionId(1000);
    flow.setStatus(Status.PREPARING);
    flow.setSubmitUser("goku");
    flow.setExecutionOptions(new ExecutionOptions());
    executableFlows.add(flow);
    when(this.executorLoader
        .fetchStaleFlowsForStatus(Status.PREPARING, this.cleaner.getValidityMap()))
        .thenReturn(executableFlows);
    when(this.executorLoader.fetchExecutableFlow(flow.getExecutionId()))
        .thenReturn(flow);

    // Skip the invocation of api gateway and just utilize finalizeFlow when cancelFlow is called.
    doAnswer(e -> {
      ExecutionControllerUtils.finalizeFlow(this.executorLoader, null, flow, "", null,
          Status.KILLED);
      return null;
    }).when(this.containerizedDispatchManager).cancelFlow(flow, flow.getSubmitUser());

    OnContainerizedExecutionEventListener onExecutionEventListener = mock(
        OnContainerizedExecutionEventListener.class);
    ExecutionControllerUtils.onExecutionEventListener = onExecutionEventListener;

    this.cleaner.cleanUpStaleFlows(Status.PREPARING);
    TimeUnit.MILLISECONDS.sleep(10);
    Assert.assertEquals(Status.KILLED, flow.getStatus());
    verify(this.containerImpl).deleteContainer(flow.getExecutionId());
    // Verify that the flow is indeed retried.
    verify(onExecutionEventListener).onExecutionEvent(flow, Constants.RESTART_FLOW);
  }

  @Test
  public void cleanUpContainersInTerminalStatuses() throws Exception {
    Set<Integer> pods = new HashSet<>();
    pods.add(1000);
    pods.add(1001);
    final ArrayList<ExecutableFlow> executableFlows = new ArrayList<>();
    final ExecutableFlow flow = new ExecutableFlow();
    flow.setExecutionId(1000);
    flow.setStatus(Status.PREPARING);
    flow.setSubmitUser("goku");
    flow.setExecutionOptions(new ExecutionOptions());
    executableFlows.add(flow);
    when(this.executorLoader
        .fetchStaleFlowsForStatus(any(Status.class), any(ImmutableMap.class)))
        .thenReturn(executableFlows);
    when(this.containerImpl.getContainersByDuration(Duration.ZERO)).thenReturn(pods);
    this.cleaner.cleanUpContainersInTerminalStatuses();
    verify(this.containerImpl).deleteContainer(1001);
    verify(this.containerImpl, times(0)).deleteContainer(1000);
  }

  @Test
  public void testGetContainersOfTerminatedFlows() throws Exception {
    Set<Integer> pods = new HashSet<>();
    pods.add(1000);
    pods.add(1001);
    // execution 1000 is alive, which means 1001 is to be terminated
    final ExecutableFlow flow = new ExecutableFlow();
    flow.setExecutionId(1000);
    flow.setStatus(Status.PREPARING);
    flow.setSubmitUser("dummy-user");
    flow.setExecutionOptions(new ExecutionOptions());
    final ArrayList<ExecutableFlow> executableFlows = new ArrayList<>();
    executableFlows.add(flow);

    when(this.executorLoader
        .fetchStaleFlowsForStatus(any(Status.class), any(ImmutableMap.class)))
        .thenReturn(executableFlows);
    when(this.containerImpl.getContainersByDuration(Duration.ZERO)).thenReturn(pods);

    Set<Integer> containersOfTerminatedFlows = this.cleaner.getContainersOfTerminatedFlows();
    Assert.assertTrue(containersOfTerminatedFlows.contains(1001));
    Assert.assertFalse(containersOfTerminatedFlows.contains(1000));
  }

  @Test
  public void testGetContainersOfTerminatedFlowsFailFetchExecutionByStatus() throws Exception {
    Set<Integer> pods = new HashSet<>();
    pods.add(1000);
    pods.add(1001);

    when(this.executorLoader
        .fetchStaleFlowsForStatus(any(Status.class), any(ImmutableMap.class)))
        .thenThrow(new ExecutorManagerException("ops"));
    when(this.containerImpl.getContainersByDuration(Duration.ZERO)).thenReturn(pods);

    Set<Integer> containersOfTerminatedFlows = this.cleaner.getContainersOfTerminatedFlows();
    Assert.assertTrue(containersOfTerminatedFlows.isEmpty());
  }

  @Test
  public void testGetRecentKilledFlows() throws ExecutorManagerException {
    final ExecutableFlow flow1 = new ExecutableFlow();
    flow1.setExecutionId(1000);
    flow1.setStatus(Status.EXECUTION_STOPPED);
    flow1.setSubmitUser("dummy-user");
    flow1.setExecutionOptions(new ExecutionOptions());
    final ArrayList<ExecutableFlow> execStoppedFlows = new ArrayList<>();
    execStoppedFlows.add(flow1);

    final ExecutableFlow flow2 = new ExecutableFlow();
    flow2.setExecutionId(2000);
    flow2.setStatus(Status.FAILED);
    flow2.setSubmitUser("dummy-user");
    flow2.setExecutionOptions(new ExecutionOptions());
    final ArrayList<ExecutableFlow> failedFlows = new ArrayList<>();
    failedFlows.add(flow2);

    when(this.executorLoader
        .fetchFreshFlowsForStatus(eq(Status.EXECUTION_STOPPED), any(ImmutableMap.class)))
        .thenReturn(execStoppedFlows);
    when(this.executorLoader
        .fetchFreshFlowsForStatus(eq(Status.FAILED), any(ImmutableMap.class)))
        .thenReturn(failedFlows);

    Set<Integer> recentTerminationFlows = this.cleaner.getRecentlyTerminatedFlows();
    Assert.assertTrue(recentTerminationFlows.contains(1000));
    Assert.assertTrue(recentTerminationFlows.contains(2000));
    Assert.assertEquals(2, recentTerminationFlows.size());
  }

  @Test
  public void testGetRecentKilledFlowsException() throws ExecutorManagerException {
    when(this.executorLoader
        .fetchFreshFlowsForStatus(any(Status.class), any(ImmutableMap.class)))
        .thenThrow(new ExecutorManagerException("ops"));

    Set<Integer> recentTerminationFlows = this.cleaner.getRecentlyTerminatedFlows();
    Assert.assertTrue(recentTerminationFlows.isEmpty());
  }

  @Test
  public void testCleanUpYarnApplicationsInClusterSucceed()
      throws Exception {
    PowerMockito.mockStatic(YarnUtils.class);
    YarnClient mockClient = mock(YarnClient.class);
    when(YarnUtils.createYarnClient(any(), any())).thenReturn(mockClient);
    ApplicationReport ap1 = mock(ApplicationReport.class),
        ap2 = mock(ApplicationReport.class),
        ap3 = mock(ApplicationReport.class);
    when(YarnUtils.getAllAliveAppReportsByExecIDs(any(), any(), any())).thenReturn(
        ImmutableList.of(ap1, ap2, ap3)
    );
    PowerMockito.doNothing().when(YarnUtils.class,
        "killApplicationAsProxyUser", any(), any(), any());

    this.cleaner.cleanUpYarnApplicationsInCluster(
        ImmutableSet.of(), new Cluster("abc", new Props()));
  }


  @Test
  public void testCleanUpYarnApplicationsInClusterKillPartialSucceed()
      throws Exception {
    PowerMockito.mockStatic(YarnUtils.class);
    YarnClient mockClient = mock(YarnClient.class);
    when(YarnUtils.createYarnClient(any(), any())).thenReturn(mockClient);
    ApplicationReport ap1 = mock(ApplicationReport.class),
        ap2 = mock(ApplicationReport.class),
        ap3 = mock(ApplicationReport.class);
    when(YarnUtils.getAllAliveAppReportsByExecIDs(any(), any(), any())).thenReturn(
        ImmutableList.of(ap1, ap2, ap3)
    );
    PowerMockito.doNothing().when(YarnUtils.class,
        "killApplicationAsProxyUser", any(), eq(ap1), any());
    PowerMockito.doNothing().when(YarnUtils.class,
        "killApplicationAsProxyUser", any(), eq(ap2), any());
    // exception
    PowerMockito.doThrow(new RuntimeException("ops")).when(YarnUtils.class,
        "killApplicationAsProxyUser", any(), eq(ap1), any());

    this.cleaner.cleanUpYarnApplicationsInCluster(
        ImmutableSet.of(), new Cluster("abc", new Props()));
  }

  @Test
  public void testCleanUpYarnApplicationsInClusterFailGetApplications()
      throws Exception {
    PowerMockito.mockStatic(YarnUtils.class);
    YarnClient mockClient = mock(YarnClient.class);
    when(YarnUtils.createYarnClient(any(), any())).thenReturn(mockClient);
    when(YarnUtils.getAllAliveAppReportsByExecIDs(any(), any(), any()))
        .thenThrow(new IOException("ops"));
//    // this will never be called
//    PowerMockito.doNothing().when(YarnUtils.class,
//        "killApplicationAsProxyUser", any(), any(), any());

    this.cleaner.cleanUpYarnApplicationsInCluster(
        ImmutableSet.of(), new Cluster("abc", new Props()));
  }
}
