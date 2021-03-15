package azkaban.executor.container.watch;

import static java.util.Objects.requireNonNull;

import azkaban.executor.container.watch.KubernetesWatch.PodWatchParams;
import com.google.common.collect.ImmutableList;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.JSON;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
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
  private final int DEFAULT_MAX_INIT_COUNT = 3;
  private final int DEFAULT_WATCH_RESET_DELAY_MILLIS = 100;
  private final int DEFAULT_WATCH_COMPLETION_TIMEOUT_MILLIS = 5000;

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
  private static String POD_WITH_LIFECYCLE_SUCCESS = "flow-pod-cluster1-280";

  private static final List<AzPodStatus> successfullFlowPodStateTransitionSequence = ImmutableList.of(
      AzPodStatus.AZ_POD_REQUESTED,
      AzPodStatus.AZ_POD_SCHEDULED,
      AzPodStatus.AZ_POD_INIT_CONTAINERS_RUNNING,
      AzPodStatus.AZ_POD_APP_CONTAINERS_STARTING,
      AzPodStatus.AZ_POD_READY,
      AzPodStatus.AZ_POD_COMPLETED);

  private ApiClient defaultApiClient;

  @Before
  public void setUp() throws Exception {
    this.defaultApiClient = Config.defaultClient();
  }

  private KubernetesWatch kubernetesWatchWithMockListener() {
    ApiClient localApiClient;
    try {
      localApiClient = ClientBuilder.kubeconfig(localKubeConfig()).build();
    } catch (IOException e) {
      final WatchException we = new WatchException("Unable to create client", e);
      logger.error("Exception reported. ", we);
      throw we;
    }
    return new KubernetesWatch(localApiClient, new AzPodStausExtractingListener(),
        new PodWatchParams(DEFAULT_NAMESPACE, null, DEFAULT_WATCH_RESET_DELAY_MILLIS));
  }

  private KubeConfig localKubeConfig() throws  IOException {
    return KubeConfig.loadKubeConfig(Files.newBufferedReader(Paths.get(LOCAL_KUBE_CONFIG_PATH), Charset.defaultCharset()));
  }

  private StatusLoggingListener statusLoggingListener() {
    return new StatusLoggingListener();
  }

  private AzPodStatusDriver statusDriverWithListener(AzPodStatusListener listener) {
    AzPodStatusDriver azPodStatusDriver = new AzPodStatusDriver();
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
    return new PreInitializedWatch(defaultApiClient,
        driver,
        podWatch,
        new PodWatchParams(null, null, DEFAULT_WATCH_RESET_DELAY_MILLIS),
        maxInitCount);
  }

  @Test
  public void testWatchShutdownAndResetAfterFailure() throws Exception {
    AzPodStatusDriver statusDriver = statusDriverWithListener(statusLoggingListener());
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

  @Test
  public void testFlowPodCompleteTransition() throws Exception {
    StatusLoggingListener loggingListener = statusLoggingListener();
    AzPodStatusDriver statusDriver = statusDriverWithListener(loggingListener);
    Watch<V1Pod> fileBackedWatch = fileBackedWatch(Config.defaultClient());
    PreInitializedWatch kubernetesWatch = defaultPreInitializedWatch(statusDriver, fileBackedWatch,
        1);
    kubernetesWatch.launchPodWatch().join(DEFAULT_WATCH_COMPLETION_TIMEOUT_MILLIS);

    // Record any information from the in-memory log of events.
    ConcurrentMap<String, Queue<AzPodStatus>> statusLogMap = loggingListener.getStatusLogMap();
    StatusLoggingListener.logDebugStatusMap(statusLogMap);

    // Verify the sequence of events received for the flow-pod {@link POD_WITH_LIFECYCLE_SUCCESS}
    // matches the expected sequence of statuses.
    List<AzPodStatus> actualLifecycleStates =
        statusLogMap.get(POD_WITH_LIFECYCLE_SUCCESS).stream()
        .distinct().collect(Collectors.toList());
    Assert.assertEquals(successfullFlowPodStateTransitionSequence, actualLifecycleStates);
    statusDriver.shutdown();
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
      this.setPodWatch(preInitPodWatch);
      if (initWatchCount >= maxInitCount) {
        logger.debug("Requesting shutdowns as max init count was reached, init-count: " + initWatchCount);
        this.requestShutdown();
      }
      initWatchCount++;
    }

    @Override
    protected void startPodWatch() throws IOException {
      startWatchCount++;
      super.startPodWatch();
    }

    public int getMaxInitCount() {
      return maxInitCount;
    }

    public int getInitWatchCount() {
      return initWatchCount;
    }

    public int getStartWatchCount() {
      return startWatchCount;
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
      reader = Files.newBufferedReader(jsonEventsFile, StandardCharsets.UTF_8);
    }

    @Override
    public Response<T> next() {
      try {
        String line = reader.readLine();
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
        return reader.ready();
      } catch (IOException e) {
        throw new RuntimeException("Exception in hasNext.", e);
      }
    }

    @Override
    public void close() throws IOException {
      if (reader != null) {
        reader.close();
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
      AzPodStatus podStatus = event.getAzPodStatus();
      String podName = event.getPodName();
      Queue<AzPodStatus> statusLog = statusLogMap.computeIfAbsent(
          podName, k -> new ConcurrentLinkedQueue<>());
      statusLog.add(podStatus);
    }

    public ConcurrentMap<String, Queue<AzPodStatus>> getStatusLogMap() {
      return statusLogMap;
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
      AzPodStatus azPodStatus = AzPodStatusExtractor.azPodStatusFromEvent(watchEvent).getAzPodStatus();
      logger.debug("AZ_POD_STATUS: " + azPodStatus);
    }
  }
}