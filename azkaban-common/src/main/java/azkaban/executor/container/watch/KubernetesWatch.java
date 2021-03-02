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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.inject.Singleton;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides primitives for 'watching' change capture events of kubernetes resources.
 * This is currently aimed at the monitoring the state changes of FlowContainer pods, but can be
 * extended for including other Kubernetes resources.
 */
@Singleton
public class KubernetesWatch {
  private static final Logger logger = LoggerFactory.getLogger(KubernetesWatch.class);

  private final ApiClient client;
  private final CoreV1Api coreV1Api;
  private final PodWatchParams podWatchParams;
  private final Thread watchRunner;
  private Watch<V1Pod> podWatch;
  private RawPodWatchEventListener podWatchEventListener;
  private int podWatchInitCount = 0;
  private AtomicBoolean isShutdownRequested = new AtomicBoolean(false);

  /**
   *  Create an instance of the watch which uses the provided kube-config and {@link PodWatchParams}
   *  configuration and registers the @{code podWatchEventListener} as the event processing
   *  implementation.
   *
   * @param kubeConfig
   * @param podWatchEventListener
   * @param podWatchParams
   */
  @Inject
  public KubernetesWatch(KubeConfig kubeConfig,
      RawPodWatchEventListener podWatchEventListener,
      PodWatchParams podWatchParams) {
    requireNonNull(kubeConfig);
    requireNonNull(podWatchEventListener);
    requireNonNull(podWatchParams);
    this.podWatchEventListener = podWatchEventListener;
    this.podWatchParams = podWatchParams;
    try {
      this.client = ClientBuilder.kubeconfig(kubeConfig).build();
    } catch (IOException e) {
      final WatchException we = new WatchException("Unable to create client", e);
      logger.error("Exception reported. ", we);
      throw we;
    }
    // no timeout for request completion
    OkHttpClient httpClient =
        client.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build();
    client.setHttpClient(httpClient);
    this.coreV1Api = new CoreV1Api(this.client);

    watchRunner = new Thread(this::initializeAndStartPodWatch);
  }

  public KubernetesWatch(KubeConfig kubeConfig,
      RawPodWatchEventListener podWatchEventListener) {
    this(kubeConfig, podWatchEventListener, new PodWatchParams(null, null));
  }

  public int getPodWatchInitCount() {
    return podWatchInitCount;
  }

  @VisibleForTesting
  protected void setPodWatch(Watch<V1Pod> podWatch) {
    requireNonNull(podWatch, "pod watch must not be null");
    this.podWatch = podWatch;
  }

  /**
   * Create the Pod watch and set it up for creating parsed representations of the JSON
   * responses received from the Kubernetes API server. Responses will be converted to type
   * {@code Watch.Response<V1Pod>}.
   * Creating the watch submits the request the API server but does not block beyond that.
   *
   * @throws ApiException
   */
  protected void initializePodWatch() throws ApiException {
    try {
      this.podWatch = Watch.createWatch(this.client,
          coreV1Api.listNamespacedPodCall(podWatchParams.getNamespace(),
              "true",
              false,
              null,
              null,
              podWatchParams.getLabelSelector(),
              null,
              null,
              null,
              true,
              null),
          new TypeToken<Response<V1Pod>>() {}.getType());
    } catch (ApiException ae) {
      logger.error("ApiException while creating pod watch.", ae);
      throw ae;
    }
  }

  /**
   * This starts the continuous event processing loop for fetching the pod watch events.
   * Processing of the events is callback driven and the registered {@link RawPodWatchEventListener}
   * provides the processing implementation.
   *
   * @throws IOException
   */
  protected void startPodWatch() throws IOException {
    requireNonNull(podWatch, "watch must be initialized");
    for (Watch.Response<V1Pod> item : podWatch) {
      if (isShutdownRequested.get()) {
        logger.info("Exiting pod watch event loop as shutdown was requested");
        return;
      }
      this.podWatchEventListener.onEvent(item);
    }
  }

  private void closePodWatchQuietly() {
    if (podWatch == null) {
      logger.debug("Pod watch is null");
      return;
    }
    try {
      podWatch.close();
    } catch (IOException e) {
      logger.error("IOException while closing pod watch.", e);
    }
  }

  /**
   * Initialize and start the Pod watch procedure. The processing is expected to continue until a
   * shutdown request is received. The method will handle any exceptions received during both (1)
   * the execution of kubernetes API calls (2) processing of events received by the watch.
   * This also includes re-initialization of the watch using Kuberentes watch API, as needed.
   */
  public void initializeAndStartPodWatch() {
    while(!isShutdownRequested.get()) {
      try {
        podWatchInitCount++;
        logger.info("Initializing pod watch, initialization count: " + podWatchInitCount);
        initializePodWatch();
        startPodWatch();
      } catch (Exception e) {
        logger.warn("Exception during pod watch was suppressed.", e);
      } finally {
        logger.info("Closing pod watch");
        closePodWatchQuietly();
      }
      logger.info("Pod watch was terminated, will be reset with delay if shutdown was not "
          + "requested. Shutdown Requested is: " + isShutdownRequested.get());

      try {
        Thread.sleep(podWatchParams.getResetDelayMillis());
      } catch (InterruptedException e) {
        if (Thread.currentThread().isInterrupted()) {
          logger.info("Pod watch reset delay was interrupted.");
        }
      }
    }
  }

  /**
   * Launch the pod watch processing in a separate thread and return its reference.
   *
   * @return
   */
  public Thread launchPodWatch() {
    if (isShutdownRequested.get()) {
      logger.error("Pod watch was launched when shutdown already in progress");
      return null;
    }
    logger.info("Starting the pod watch thread");
    watchRunner.start();
    return watchRunner;
  }

  /**
   * Submit request for shutting down any watch processing. If shutdown has already been requested,
   * subsequent invocations of this method will be ignored.
   *
   * @return 'true' if the shutdown was not already requested, 'false' otherwise.
   */
  public boolean requestShutdown() {
    boolean notAlreadyRequested = isShutdownRequested.compareAndSet(false, true);
    if (!notAlreadyRequested) {
      logger.warn("Shutdown of kubernetes watch was already requested");
      return notAlreadyRequested;
    }
    logger.info("Requesting shutdown for kubernetes watch");
    watchRunner.interrupt();
    return notAlreadyRequested;
  }

  /**
   * Parameters used for setting up the Pod watch.
   */
  public static class PodWatchParams {
    private final static int DEFAULT_RESET_DELAY_MILLIS = 10 * 1000; //30 seconds
    private final String namespace;
    private final String labelSelector;
    private final int resetDelayMillis;

    public PodWatchParams(String namespace, String labelSelector, int resetDelayMillis) {
      this.namespace = namespace;
      this.labelSelector = labelSelector;
      this.resetDelayMillis = resetDelayMillis;
    }

    public PodWatchParams(String namespace, String labelSelector) {
      this(namespace, labelSelector, DEFAULT_RESET_DELAY_MILLIS);
    }

    public String getNamespace() {
      return namespace;
    }

    public String getLabelSelector() {
      return labelSelector;
    }

    public int getResetDelayMillis() {
      return resetDelayMillis;
    }
  }
}
