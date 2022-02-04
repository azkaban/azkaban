/*
 * Copyright 2021 LinkedIn Corp.
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
package azkaban.executor.container.watch;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.Constants.FlowParameters;
import azkaban.DispatchMethod;
import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.executor.AlerterHolder;
import azkaban.executor.DummyEventListener;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionControllerUtils;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.FlowStatusChangeEventListener;
import azkaban.executor.OnContainerizedExecutionEventListener;
import azkaban.executor.Status;
import azkaban.executor.container.ContainerizedImpl;
import azkaban.executor.container.watch.KubernetesWatch.PodWatchParams;
import azkaban.metrics.ContainerizationMetrics;
import azkaban.metrics.DummyContainerizationMetricsImpl;
import azkaban.spi.EventType;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.JSON;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesWatchTest {

  private static final Logger logger = LoggerFactory.getLogger(KubernetesWatchTest.class);
  private static String LOCAL_KUBE_CONFIG_PATH = "/path/to/valid/kube-config";
  private static final int DEFAULT_MAX_INIT_COUNT = 3;
  private static final int DEFAULT_WATCH_RESET_DELAY_MILLIS = 100;
  private static final int DEFAULT_WATCH_COMPLETION_TIMEOUT_MILLIS = 10000;

  /**
   * File containing a mock sequence of events, this is expected to be reflective of actual events
   * from a Kubernets ApiServer and will be used for providing events through a
   * {@link FileBackedWatch}.
   *
   * Notes about the file format.
   *  - {@code Watch<T>} and there by {@code FileBackedWatch} expect the response objects for
   *  individual pods to be separated by newlines. Therefore the resource file is not
   *  pretty-printed.
   *  - A quick way of pretty-printing the file: {@code cat $filename| jq .}
   */
  private static String JSON_EVENTS_FILE_PATH =
      KubernetesWatch.class.getResource("sample-events1.json").getPath();

  /**
   * These constants refer to kubernetes objects with the mock events file at
   * {@code JSON_EVENTS_FILEP_PATH}.
   */
  private static final String DEFAULT_NAMESPACE = "dev-namespace1";
  private static final String DEFAULT_CLUSTER = "cluster1";
  private static String PODNAME_WITH_SUCCESS = "flow-pod-cluster1-280";
  private static String PODNAME_WITH_INIT_FAILURE = "flow-pod-cluster1-740";
  private static String PODNAME_WITH_INVALID_TRANSITIONS = "flow-pod-cluster1-999";
  private static int EXECUTION_ID_WITH_SUCCEESS = 280;
  private static int EXECUTION_ID_WITH_INIT_FAILURE = 740;
  private static int EXECUTION_ID_WITH_CREATE_CONTAINER_ERROR = 420;
  private static int EXECUTION_ID_WITH_INVALID_TRANSITIONS = 999;

  private static final String DEFAULT_PROJECT_NAME = "exectest1";
  private static final String DEFAULT_FLOW_NAME = "exec1";

  private static final ImmutableList<AzPodStatus> TRANSITION_SEQUENCE_WITH_SUCCESS = ImmutableList.of(
      AzPodStatus.AZ_POD_REQUESTED,
      AzPodStatus.AZ_POD_SCHEDULED,
      AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING,
      AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING,
      AzPodStatus.AZ_POD_READY,
      AzPodStatus.AZ_POD_COMPLETED);

  private ApiClient defaultApiClient;
  private ContainerizationMetrics containerizationMetrics = new DummyContainerizationMetricsImpl();
  private OnContainerizedExecutionEventListener onExecutionEventListener = mock(
      OnContainerizedExecutionEventListener.class);
  private Map<String, String> flowParam = ImmutableMap.of(FlowParameters
      .FLOW_PARAM_ALLOW_RESTART_ON_EXECUTION_STOPPED, "true");
  private EventListener eventListener = new DummyEventListener();

  @Before
  public void setUp() throws Exception {
    this.defaultApiClient = Config.defaultClient();
    ExecutionControllerUtils.onExecutionEventListener = onExecutionEventListener;
  }

  private KubernetesWatch kubernetesWatchWithMockListener() {
    Props azkProps = localProperties();
    ApiClient localApiClient = WatchUtils.createApiClient(azkProps);
    return new KubernetesWatch(localApiClient, new AzPodStausExtractingListener(),
        WatchUtils.createPodWatchParams(azkProps)
    );
  }

  private Props localProperties() {
    Props props = new Props();
    props.put(ContainerizedDispatchManagerProperties.KUBERNETES_NAMESPACE, DEFAULT_NAMESPACE);
    props.put(ConfigurationKeys.AZKABAN_CLUSTER_NAME, DEFAULT_CLUSTER);
    props.put(ContainerizedDispatchManagerProperties.KUBERNETES_KUBE_CONFIG_PATH,
        LOCAL_KUBE_CONFIG_PATH);
    return props;
  }

  private StatusLoggingListener statusLoggingListener() {
    return new StatusLoggingListener();
  }

  private ExecutorLoader mockedExecutorLoader() {
    return mock(ExecutorLoader.class);
  }

  private ContainerizedImpl mockedContainerizedImpl() {
    return mock(ContainerizedImpl.class);
  }

  private ExecutableFlow createExecutableFlow(int executionId,
      azkaban.executor.Status flowStatus,
      String flowName,
      String projectName) throws Exception {
    ExecutableFlow flow = TestUtils.createTestExecutableFlow(projectName, flowName,
        DispatchMethod.CONTAINERIZED);
    flow.setExecutionId(executionId);
    flow.setStatus(flowStatus);
    return flow;
  }

  private ExecutableFlow createExecutableFlow(int executionId,
      azkaban.executor.Status flowStatus) throws Exception {
    return createExecutableFlow(executionId, flowStatus, DEFAULT_FLOW_NAME, DEFAULT_PROJECT_NAME);
  }

  private FlowStatusManagerListener flowStatusUpdatingListener(Props azkProps) {
    return new FlowStatusManagerListener(azkProps, mockedContainerizedImpl(),
        mockedExecutorLoader(), mock(AlerterHolder.class), containerizationMetrics, eventListener);
  }

  private AzPodStatusDrivingListener statusDriverWithListener(AzPodStatusListener listener) {
    AzPodStatusDrivingListener azPodStatusDriver = new AzPodStatusDrivingListener(new Props());
    azPodStatusDriver.registerAzPodStatusListener(listener);
    return azPodStatusDriver;
  }


  private Watch<V1Pod> fileBackedWatch(ApiClient apiClient) throws  IOException {
    FileBackedWatch<V1Pod> fileBackedWatch = new FileBackedWatch<>(
        apiClient.getJSON(),
        new TypeToken<Response<V1Pod>>() {}.getType(),
        Paths.get(JSON_EVENTS_FILE_PATH));
    return fileBackedWatch;
  }

  private PreInitializedWatch defaultPreInitializedWatch(RawPodWatchEventListener driver,
      Watch<V1Pod> podWatch,
      int maxInitCount) throws IOException {
    return new PreInitializedWatch(this.defaultApiClient,
        driver,
        podWatch,
        new PodWatchParams(null, null, DEFAULT_WATCH_RESET_DELAY_MILLIS),
        maxInitCount);
  }

  @Test
  public void testWatchShutdownAndResetAfterFailure() throws Exception {
    AzPodStatusDrivingListener statusDriver = statusDriverWithListener(statusLoggingListener());
    Watch<V1Pod> fileBackedWatch = fileBackedWatch(Config.defaultClient());
    PreInitializedWatch kubernetesWatch = defaultPreInitializedWatch(statusDriver, fileBackedWatch,
        DEFAULT_MAX_INIT_COUNT);

    // FileBackedWatch with throw an IOException at read once the end of file has been reached.
    // PreInitWatch will auto-shutdown after the max_init_count resets.
    kubernetesWatch.launchPodWatch().join(DEFAULT_WATCH_COMPLETION_TIMEOUT_MILLIS);

    // Max watch init count should be less than or equal to the actual init count. Actual init
    // count can be larger by 1 as it may take upto 1 reset cycle for the shutdown request to be
    // evaluated.
    Assert.assertTrue(DEFAULT_MAX_INIT_COUNT <=  kubernetesWatch.getInitWatchCount());
    Assert.assertTrue(DEFAULT_MAX_INIT_COUNT <=  kubernetesWatch.getStartWatchCount());
    statusDriver.shutdown();
  }

  private void assertPodEventSequence(String podName,
      StatusLoggingListener loggingListener,
      List<AzPodStatus> transitionSequence) {
    // Record any information from the in-memory log of events.
    ConcurrentMap<String, Queue<AzPodStatus>> statusLogMap = loggingListener.getStatusLogMap();
    StatusLoggingListener.logDebugStatusMap(statusLogMap);

    // Verify the sequence of events received for the flow-pod {@link POD_WITH_LIFECYCLE_SUCCESS}
    // matches the expected sequence of statuses.
    List<AzPodStatus> actualLifecycleStates =
        statusLogMap.get(podName).stream()
            .distinct().collect(Collectors.toList());
    Assert.assertEquals(transitionSequence, actualLifecycleStates);
  }

  @Test
  public void testPodEventSequenceSuccess() throws Exception {
    StatusLoggingListener loggingListener = statusLoggingListener();
    AzPodStatusDrivingListener statusDriver = statusDriverWithListener(loggingListener);
    Watch<V1Pod> fileBackedWatch = fileBackedWatch(Config.defaultClient());
    PreInitializedWatch kubernetesWatch = defaultPreInitializedWatch(statusDriver, fileBackedWatch,
        1);
    kubernetesWatch.launchPodWatch().join(DEFAULT_WATCH_COMPLETION_TIMEOUT_MILLIS);

    assertPodEventSequence(PODNAME_WITH_SUCCESS, loggingListener, TRANSITION_SEQUENCE_WITH_SUCCESS);
    statusDriver.shutdown();
  }

  // Validates that the FlowUpdatingListener can:
  //   1. Delete the PODs in completed state.
  //   2. Finalize the corresponding flow execution (in case not already in a final state)
  @Test
  public void testFlowStatusManagerListenerTransitionCompleted() throws Exception {
    // Setup a FlowUpdatingListener
    Props azkProps = new Props();
    FlowStatusManagerListener updatingListener = flowStatusUpdatingListener(azkProps);

    // Add a StatusLoggingListener for sanity check the events sequence. This also validates
    // support for registering multiple listeners.
    StatusLoggingListener loggingListener = statusLoggingListener();
    AzPodStatusDrivingListener statusDriver = new AzPodStatusDrivingListener(azkProps);
    statusDriver.registerAzPodStatusListener(loggingListener);
    statusDriver.registerAzPodStatusListener(updatingListener);

    // Mocked flow in RUNNING state. Pod completion event will be processed for this execution.
    ExecutableFlow flow1 = createExecutableFlow(EXECUTION_ID_WITH_SUCCEESS, Status.RUNNING);
    when(updatingListener.getExecutorLoader().fetchExecutableFlow(EXECUTION_ID_WITH_SUCCEESS))
        .thenReturn(flow1);

    // Run all the events through the registered listeners.
    Watch<V1Pod> fileBackedWatch = fileBackedWatch(Config.defaultClient());
    PreInitializedWatch kubernetesWatch = defaultPreInitializedWatch(statusDriver, fileBackedWatch,
        1);
    kubernetesWatch.launchPodWatch().join(DEFAULT_WATCH_COMPLETION_TIMEOUT_MILLIS);

    // Verify that the previously RUNNING flow has been finalized to a failure state.
    verify(updatingListener.getExecutorLoader()).updateExecutableFlow(flow1);
    assertThat(flow1.getStatus()).isEqualTo(Status.EXECUTION_STOPPED);

    // Verify the Pod deletion API is invoked.
    verify(updatingListener.getContainerizedImpl()).deleteContainer(flow1);

    // Sanity check for asserting the sequence in which events were received.
    assertPodEventSequence(PODNAME_WITH_SUCCESS, loggingListener, TRANSITION_SEQUENCE_WITH_SUCCESS);
  }

  @Test
  public void testFlowManagerListenerInitContainerFailure() throws Exception {
    // Setup a FlowUpdatingListener
    Props azkProps = new Props();
    FlowStatusManagerListener updatingListener = flowStatusUpdatingListener(azkProps);
    // Verify EXECUTION_STOPPED flow life cycle event is emitted
    assertExecutionStoppedFlowEvent(updatingListener);
    AzPodStatusDrivingListener statusDriver = new AzPodStatusDrivingListener(azkProps);
    statusDriver.registerAzPodStatusListener(updatingListener);

    // Mocked flow in RUNNING state. Init failure event will be processed for this execution.
    ExecutableFlow flow1 = createExecutableFlow(EXECUTION_ID_WITH_INIT_FAILURE, Status.PREPARING);

    when(updatingListener.getExecutorLoader().fetchExecutableFlow(EXECUTION_ID_WITH_INIT_FAILURE))
        .thenReturn(flow1);

    // Run events through the registered listeners.
    Watch<V1Pod> fileBackedWatch = fileBackedWatch(Config.defaultClient());
    PreInitializedWatch kubernetesWatch = defaultPreInitializedWatch(statusDriver, fileBackedWatch,
        1);
    kubernetesWatch.launchPodWatch().join(DEFAULT_WATCH_COMPLETION_TIMEOUT_MILLIS);

    // Verify that the previously RUNNING flow has been finalized to a failure state.
    verify(updatingListener.getExecutorLoader()).updateExecutableFlow(flow1);
    assertFlowExecutionStopped(flow1);

    // Verify the Pod deletion API is invoked.
    verify(updatingListener.getContainerizedImpl()).deleteContainer(flow1);

    // Verify that the flow is restarted.
    verify(onExecutionEventListener).onExecutionEvent(flow1, Constants.RESTART_FLOW);
  }

  @Test
  public void testFlowManagerListenerCreateContainerError() throws Exception {
    // Setup a FlowUpdatingListener
    Props azkProps = new Props();
    FlowStatusManagerListener updatingListener = flowStatusUpdatingListener(azkProps);
    // Verify EXECUTION_STOPPED flow life cycle event is emitted
    assertExecutionStoppedFlowEvent(updatingListener);
    AzPodStatusDrivingListener statusDriver = new AzPodStatusDrivingListener(azkProps);
    statusDriver.registerAzPodStatusListener(updatingListener);

    // Mocked flow in DISPATCHING state. CreateContainerError event will be processed for this
    // execution. The extracted AzPodStatus will be AZ_POD_APP_FAILURE.
    ExecutableFlow flow1 = createExecutableFlow(EXECUTION_ID_WITH_CREATE_CONTAINER_ERROR,
        Status.DISPATCHING);
    when(updatingListener.getExecutorLoader()
        .fetchExecutableFlow(EXECUTION_ID_WITH_CREATE_CONTAINER_ERROR))
        .thenReturn(flow1);

    // Run events through the registered listeners.
    Watch<V1Pod> fileBackedWatch = fileBackedWatch(Config.defaultClient());
    PreInitializedWatch kubernetesWatch = defaultPreInitializedWatch(statusDriver, fileBackedWatch,
        1);
    kubernetesWatch.launchPodWatch().join(DEFAULT_WATCH_COMPLETION_TIMEOUT_MILLIS);

    // Verify that the previously DISPATCHING flow has been finalized to a failure state.
    verify(updatingListener.getExecutorLoader()).updateExecutableFlow(flow1);
    assertFlowExecutionStopped(flow1);

    // Verify the Pod deletion API is invoked.
    verify(updatingListener.getContainerizedImpl())
        .deleteContainer(flow1);

    // Verify that the flow is restarted.
    verify(onExecutionEventListener).onExecutionEvent(flow1, Constants.RESTART_FLOW);
  }

  // Validates that the callbacks are processed in ContainerStatusMetricsHandlerListener
  @Test
  public void testContainerStatusMetricsListener() throws Exception {
    // Setup a ContainerStatusMetricsHandlerListener
    Props azkProps = new Props();
    AzPodStatusMetricsListener recordHandlerListener =
        new AzPodStatusMetricsListener(new DummyContainerizationMetricsImpl());

    // Register ContainerStatusMetricsHandlerListener
    AzPodStatusDrivingListener statusDriver = new AzPodStatusDrivingListener(azkProps);
    statusDriver.registerAzPodStatusListener(recordHandlerListener);

    // Run all the events through the registered listeners.
    Watch<V1Pod> fileBackedWatch = fileBackedWatch(Config.defaultClient());
    PreInitializedWatch kubernetesWatch = defaultPreInitializedWatch(statusDriver, fileBackedWatch,
        1);
    kubernetesWatch.launchPodWatch().join(DEFAULT_WATCH_COMPLETION_TIMEOUT_MILLIS);

    // Verify pod statuses are handled by ContainerStatusMetricsHandlerListener to emit status
    //metrics. In total there are 15 events, of which some Scheduled and InitContainersRunning are
    //duplicated event statuses.
    assertThat(recordHandlerListener.getPodRequestedCounter()).isEqualTo(2);
    assertThat(recordHandlerListener.getPodScheduledCounter()).isEqualTo(3);
    assertThat(recordHandlerListener.getPodInitContainersRunningCounter()).isEqualTo(2);
    assertThat(recordHandlerListener.getPodAppContainersStartingCounter()).isEqualTo(2);
    assertThat(recordHandlerListener.getPodReadyCounter()).isEqualTo(1);
    assertThat(recordHandlerListener.getPodCompletedCounter()).isEqualTo(1);
    assertThat(recordHandlerListener.getPodInitFailureCounter()).isEqualTo(1);
  }

  // Validate that for invalid pod transitions corresponding flows are finalized and containers
  // are deleted.
  @Test
  public void testFlowManagerListenerInvalidTransition() throws Exception {
    // Setup a FlowUpdatingListener
    Props azkProps = new Props();
    FlowStatusManagerListener updatingListener = flowStatusUpdatingListener(azkProps);
    // Verify EXECUTION_STOPPED flow life cycle event is emitted
    assertExecutionStoppedFlowEvent(updatingListener);
    AzPodStatusDrivingListener statusDriver = new AzPodStatusDrivingListener(azkProps);
    statusDriver.registerAzPodStatusListener(updatingListener);

    // Mocked flow in RUNNING state.
    ExecutableFlow flow1 = TestUtils.createTestExecutableFlowFromYaml("embeddedflowyamltest",
        "embedded_flow");
    flow1.setExecutionId(EXECUTION_ID_WITH_INVALID_TRANSITIONS);
    flow1.setStatus(Status.RUNNING);
    // set flow parameter to allow restart from EXECUTION_STOPPED
    final ExecutionOptions options = flow1.getExecutionOptions();
    options.addAllFlowParameters(flowParam);
    flow1.setExecutionOptions(options);
    when(updatingListener.getExecutorLoader().fetchExecutableFlow(EXECUTION_ID_WITH_INVALID_TRANSITIONS))
        .thenReturn(flow1);

    // Process events through the registered listeners.
    Watch<V1Pod> fileBackedWatch = fileBackedWatch(Config.defaultClient());
    PreInitializedWatch kubernetesWatch = defaultPreInitializedWatch(statusDriver, fileBackedWatch,
        1);
    kubernetesWatch.launchPodWatch().join(DEFAULT_WATCH_COMPLETION_TIMEOUT_MILLIS);

    // Verify that the previously RUNNING flow has been finalized to a terminal state, and sub
    // nodes set to terminal state, too.
    verify(updatingListener.getExecutorLoader()).updateExecutableFlow(flow1);
    assertFlowExecutionStopped(flow1);

    // Verify the Pod deletion API is invoked.
    verify(updatingListener.getContainerizedImpl()).deleteContainer(flow1);

    // Verify that the flow is restarted.
    verify(onExecutionEventListener).onExecutionEvent(flow1, Constants.RESTART_FLOW);
  }

  //// Verify FLOW_FINISHED flow life cycle event with status EXECUTION_STOPPED is emitted
  private void assertExecutionStoppedFlowEvent(final FlowStatusManagerListener updatingListener) {
    updatingListener.addListener((event) -> {
      Event flowEvent = (Event) event;
      Assert.assertEquals(EventType.FLOW_FINISHED, flowEvent.getType());
      Assert.assertEquals(Status.EXECUTION_STOPPED, flowEvent.getData().getStatus());
    });
  }

  // validate flow status is finalized to EXECUTION_STOPPED, all sub nodes are set to KILLED
  private void assertFlowExecutionStopped(final ExecutableFlow flow) {
    final Queue<ExecutableNode> queue = new LinkedList<>();
    queue.add(flow);
    // traverse through every node in flow1
    while(!queue.isEmpty()) {
      ExecutableNode node = queue.poll();
      if (node==flow) {
        assertThat(node.getStatus()).isEqualTo(Status.EXECUTION_STOPPED);
      } else {
        assertThat(node.getStatus()).isEqualTo(Status.KILLED);
      }
      if (node instanceof ExecutableFlowBase) {
        final ExecutableFlowBase base = (ExecutableFlowBase) node;
        for (final ExecutableNode subNode : base.getExecutableNodes()) {
          queue.add(subNode);
        }
      }
    }
  }

  @Test
  @Ignore("Blocking watch execution, useful only for development")
  public void testBlockingPodWatch() throws Exception {
    // Runs an unmodified instance of the watch that logs Raw watch events with debug verbosity.
    KubernetesWatch kubernetesWatch = kubernetesWatchWithMockListener();
    kubernetesWatch.launchPodWatch().join();
  }

  /**
   * An extension to {@link KubernetesWatch} which requires the underlying {@link Watch<V1Pod>}
   * provider to be pre-initialized. This is the case for the {@link FileBackedWatch}, for
   * example. This also disables watch initialization in the parent class and additionally maintains
   * counters for how may times the watch initialization and startup routines were invoked.
   */
  private static class PreInitializedWatch extends KubernetesWatch {
    private final Watch<V1Pod> preInitPodWatch;
    private final int maxInitCount;
    private int initWatchCount = 0;
    private int startWatchCount = 0;

    public PreInitializedWatch(ApiClient apiClient,
        RawPodWatchEventListener podWatchEventListener,
        Watch<V1Pod> preInitPodWatch,
        PodWatchParams podWatchParams,
        int maxInitCount) {
      super(apiClient, podWatchEventListener, podWatchParams);
      requireNonNull(preInitPodWatch, "pre init pod watch must not be null");
      this.preInitPodWatch = preInitPodWatch;
      this.maxInitCount = maxInitCount;
    }

    @Override
    protected void initializePodWatch() {
      this.setPodWatch(this.preInitPodWatch);
      if (this.initWatchCount >= this.maxInitCount) {
        logger.debug("Requesting shutdowns as max init count was reached, init-count: " + this.initWatchCount);
        this.requestShutdown();
      }
      this.initWatchCount++;
    }

    @Override
    protected void startPodWatch() throws IOException {
      this.startWatchCount++;
      super.startPodWatch();
    }

    public int getMaxInitCount() {
      return this.maxInitCount;
    }

    public int getInitWatchCount() {
      return this.initWatchCount;
    }

    public int getStartWatchCount() {
      return this.startWatchCount;
    }
  }

  /**
   * For providing watch events from a file, instead of the kubernetes API server.
   * This serves 2 main purposes:
   *   (1) Extension of {@link Watch<T>} and can be used as a drop-in replacement within
   *    {@link KubernetesWatch}
   *   (2) Has the ability to parse JSON objects and convert them to {@link Watch<T>} events
   *    which are identical to the events received directly from the ApiServer.
   * @param <T>
   */
  private static class FileBackedWatch<T> extends Watch<T> {
    private final BufferedReader reader;

    public FileBackedWatch(JSON json, Type watchType, Path jsonEventsFile) throws IOException {
      super(json, null, watchType, null);
      requireNonNull(jsonEventsFile);
      this.reader = Files.newBufferedReader(jsonEventsFile, StandardCharsets.UTF_8);
    }

    @Override
    public Response<T> next() {
      try {
        String line = this.reader.readLine();
        if (line == null) {
          throw new RuntimeException("Line is null");
        }
        return parseLine(line);
      } catch (IOException e) {
        throw new RuntimeException("IO Exception during next method.", e);
      }
    }

    @Override
    public boolean hasNext() {
      try {
        return this.reader.ready();
      } catch (IOException e) {
        throw new RuntimeException("Exception in hasNext.", e);
      }
    }

    @Override
    public void close() throws IOException {
      if (this.reader != null) {
        this.reader.close();
      }
    }
  }

  /**
   * A simple implementation of the {@link AzPodStatusListener} that will keep an in-memory log
   * of all the event received.
   * Additionally, contains routines for printing the in-memory log for helping with debug.
   */
  private static class StatusLoggingListener implements AzPodStatusListener {
    private final ConcurrentMap<String, Queue<AzPodStatus>> statusLogMap =
        new ConcurrentHashMap<>();

    /**
     * Print the event log at debug verbosity.
     * @param statusLogMap
     */
    public static void logDebugStatusMap(ConcurrentMap<String, Queue<AzPodStatus>> statusLogMap) {
      requireNonNull(statusLogMap, "status log map must not be null");
      statusLogMap.forEach((podname, queue) -> {
        StringBuilder sb = new StringBuilder(podname + ": ");
        queue.forEach(status -> sb.append(status.toString() + ", "));
        logger.debug(sb.toString());
      });
    }

    /**
     * Log status for the given event.
     * @param event
     */
    private void logStatusForPod(AzPodStatusMetadata event) {
      requireNonNull(event, "event must not be null");
      AzPodStatus podStatus = event.getAzPodStatus();
      String podName = event.getPodName();
      Queue<AzPodStatus> statusLog = this.statusLogMap.computeIfAbsent(
          podName, k -> new ConcurrentLinkedQueue<>());
      statusLog.add(podStatus);
    }

    public ConcurrentMap<String, Queue<AzPodStatus>> getStatusLogMap() {
      return this.statusLogMap;
    }

    @Override
    public void onPodRequested(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodScheduled(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodInitContainersRunning(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodAppContainersStarting(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodReady(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodCompleted(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodInitFailure(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodAppFailure(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }

    @Override
    public void onPodUnexpected(AzPodStatusMetadata event) {
      logStatusForPod(event);
    }
  }

  /**
   * An implementation of {@link @RawPodWatchListener} that extracts the {@link AzPodStatus} for
   * each pod watch event.
   */
  private static class AzPodStausExtractingListener implements RawPodWatchEventListener {
    @Override
    public void onEvent(Response<V1Pod> watchEvent) {
      logger.debug(String.format("%s : %s, %s, %s", watchEvent.type, watchEvent.object.getMetadata().getName(),
          watchEvent.object.getStatus().getMessage(), watchEvent.object.getStatus().getPhase()));
      AzPodStatus azPodStatus = AzPodStatusExtractor.getAzPodStatusFromEvent(watchEvent).getAzPodStatus();
      logger.debug("AZ_POD_STATUS: " + azPodStatus);
    }
  }

  /**
   * A class extends {@link ContainerStatusMetricsListener} that can be tested for metrics
   * updating
   */
  private static class AzPodStatusMetricsListener extends ContainerStatusMetricsListener {
    private int podRequestedCounter =0, podScheduledCounter = 0, podInitContainersRunningCounter = 0,
        PodAppContainersStartingCounter = 0, podReadyCounter = 0, podCompletedCounter = 0,
        podInitFailureCounter = 0, podAppFailureCounter = 0, podUnexpectedCounter = 0;
    public AzPodStatusMetricsListener(
        ContainerizationMetrics containerizationMetrics) {
      super(containerizationMetrics);
    }

    @Override
    public synchronized void onPodRequested(final AzPodStatusMetadata event) {
      if (!event.getFlowPodMetadata().isPresent() || isUpdatedPodStatusDistinct(event)) {
        podRequestedCounter++;
        updatePodStatus(event);
      }
    }

    @Override
    public synchronized void onPodScheduled(final AzPodStatusMetadata event) {
      if (!event.getFlowPodMetadata().isPresent() || isUpdatedPodStatusDistinct(event)) {
        podScheduledCounter++;
        updatePodStatus(event);
      }
    }

    @Override
    public synchronized void onPodInitContainersRunning(final AzPodStatusMetadata event) {
      if (!event.getFlowPodMetadata().isPresent() || isUpdatedPodStatusDistinct(event)) {
        podInitContainersRunningCounter++;
        updatePodStatus(event);
      }
    }

    @Override
    public synchronized void onPodAppContainersStarting(final AzPodStatusMetadata event) {
      if (!event.getFlowPodMetadata().isPresent() || isUpdatedPodStatusDistinct(event)) {
        PodAppContainersStartingCounter++;
        updatePodStatus(event);
      }
    }

    @Override
    public synchronized void onPodReady(final AzPodStatusMetadata event) {
      if (!event.getFlowPodMetadata().isPresent() || isUpdatedPodStatusDistinct(event)) {
        podReadyCounter++;
        updatePodStatus(event);
      }
    }

    @Override
    public synchronized void onPodCompleted(final AzPodStatusMetadata event) {
      if (!event.getFlowPodMetadata().isPresent() || isUpdatedPodStatusDistinct(event)) {
        podCompletedCounter++;
        updatePodStatus(event);
      }
    }

    @Override
    public synchronized void onPodInitFailure(final AzPodStatusMetadata event) {
      if (!event.getFlowPodMetadata().isPresent() || isUpdatedPodStatusDistinct(event)) {
        podInitFailureCounter++;
        updatePodStatus(event);
      }
    }

    @Override
    public synchronized void onPodAppFailure(final AzPodStatusMetadata event) {
      if (!event.getFlowPodMetadata().isPresent() || isUpdatedPodStatusDistinct(event)) {
        podAppFailureCounter++;
        updatePodStatus(event);
      }
    }

    @Override
    public void onPodUnexpected(AzPodStatusMetadata event) {
      super.onPodUnexpected(event);
    }

    public int getPodRequestedCounter() {
      return podRequestedCounter;
    }

    public int getPodScheduledCounter() { return podScheduledCounter; }

    public int getPodInitContainersRunningCounter() {
      return podInitContainersRunningCounter;
    }

    public int getPodAppContainersStartingCounter() {
      return PodAppContainersStartingCounter;
    }

    public int getPodReadyCounter() {
      return podReadyCounter;
    }

    public int getPodCompletedCounter() {
      return podCompletedCounter;
    }

    public int getPodInitFailureCounter() {
      return podInitFailureCounter;
    }

    public int getPodAppFailureCounter() {
      return podAppFailureCounter;
    }
  }
}
