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

import azkaban.executor.container.ContainerizedWatch;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.inject.Singleton;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides primitives for 'watching' change capture events of kubernetes resources.
 * This is currently aimed at the monitoring the state changes of FlowContainer pods, but can be
 * extended for including other Kubernetes resources.
 */
@Singleton
public class KubernetesWatch implements ContainerizedWatch {
  private static final Logger logger = LoggerFactory.getLogger(KubernetesWatch.class);

  private final ApiClient apiClient;
  private final CoreV1Api coreV1Api;
  private final PodWatchParams podWatchParams;
  private final Thread watchRunner;
  private Watch<V1Pod> podWatch;
  private RawPodWatchEventListener podWatchEventListener;
  private int podWatchInitCount = 0;
  private volatile boolean isLaunched = false;
  private AtomicBoolean isShutdownRequested = new AtomicBoolean(false);

  /**
   *  Create an instance of the watch which uses the provided kube-config and {@link PodWatchParams}
   *  configuration and registers the @{code podWatchEventListener} as the event processing
   *  implementation.
   *
   * @param apiClient
   * @param podWatchEventListener
   * @param podWatchParams
   */
  @Inject
  public KubernetesWatch(ApiClient apiClient,
      RawPodWatchEventListener podWatchEventListener,
      PodWatchParams podWatchParams) {
    requireNonNull(apiClient);
    requireNonNull(podWatchEventListener);
    requireNonNull(podWatchParams);

    logger.info("Creating kubernetes resource watch");
    this.podWatchEventListener = podWatchEventListener;
    this.podWatchParams = podWatchParams;
    this.apiClient = apiClient;
    // no timeout for request completion
    OkHttpClient httpClient =
        this.apiClient.getHttpClient().newBuilder()
            .protocols(Arrays.asList(Protocol.HTTP_2,Protocol.HTTP_1_1))
            .connectTimeout(60, TimeUnit.SECONDS)
            .callTimeout(1200, TimeUnit.SECONDS)
            .readTimeout(600, TimeUnit.SECONDS)
            .build();
    this.apiClient.setHttpClient(httpClient);
    this.coreV1Api = new CoreV1Api(this.apiClient);

    this.watchRunner = new Thread(this::initializeAndStartPodWatch);
  }

  public int getPodWatchInitCount() {
    return this.podWatchInitCount;
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
      this.podWatch = Watch.createWatch(this.apiClient,
          this.coreV1Api.listNamespacedPodCall(
              this.podWatchParams.getNamespace(),
              "true",
              false,
              null,
              null,
              this.podWatchParams.getLabelSelector(),
              null,
              null,
              null,
              null,
              null ,
              null),
          new TypeToken<Response<V1Pod>>() {}.getType());
    } catch (ApiException ae) {
      logger.error("ApiException while creating pod watch.", ae);
      throw ae;
    }
    logger.info("Pod watch was initialized.");
  }

  /**
   * This starts the continuous event processing loop for fetching the pod watch events.
   * Processing of the events is callback driven and the registered {@link RawPodWatchEventListener}
   * provides the processing implementation.
   *
   * @throws IOException
   */
  protected void startPodWatch() throws IOException {
    requireNonNull(this.podWatch, "watch must be initialized");
    logger.info("Starting Pod watch loop.");
    for (Watch.Response<V1Pod> item : this.podWatch) {
      if (this.isShutdownRequested.get()) {
        logger.info("Exiting pod watch event loop as shutdown was requested");
        return;
      }
      this.podWatchEventListener.onEvent(item);
    }
  }

  private void closePodWatchQuietly() {
    if (this.podWatch == null) {
      logger.debug("Pod watch is null");
      return;
    }
    try {
      this.podWatch.close();
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
  private void initializeAndStartPodWatch() {
    while(!this.isShutdownRequested.get()) {
      try {
        this.podWatchInitCount++;
        logger.info("Initializing pod watch, initialization count: " + this.podWatchInitCount);
        initializePodWatch();
        startPodWatch();
      } catch (Exception e) {
        logger.warn("Exception during pod watch was suppressed.", e);
      } finally {
        logger.info("Closing pod watch");
        closePodWatchQuietly();
      }
      logger.info("Pod watch was terminated, will be reset with delay if shutdown was not "
          + "requested. Shutdown Requested is: " + this.isShutdownRequested.get());

      try {
        Thread.sleep(this.podWatchParams.getResetDelayMillis());
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
   * @return reference to watch processing thread
   * @throws AzkabanWatchException if watch can't be launched.
   */
  @VisibleForTesting
  public synchronized Thread launchPodWatch() {
    if (this.isLaunched) {
      AzkabanWatchException awe = new AzkabanWatchException("Pod watch has already been launched");
      logger.error("Pod watch launch failure", awe);
      throw awe;
    }
    if (this.isShutdownRequested.get()) {
      AzkabanWatchException awe = new AzkabanWatchException(
          "Pod watch was launched when shutdown already in progress");
      logger.error("Pod watch launch failure", awe);
      throw awe;
    }
    logger.info("Starting the pod watch thread");
    this.watchRunner.start();
    this.isLaunched = true;
    return this.watchRunner;
  }

  @Override
  public void launchWatch() {
    this.launchPodWatch();
  }

  /**
   * Submit request for shutting down any watch processing. If shutdown has already been requested,
   * subsequent invocations of this method will be ignored.
   *
   * @return 'true' if the shutdown was not already requested, 'false' otherwise.
   */
  @Override
  public boolean requestShutdown() {
    boolean notAlreadyRequested = this.isShutdownRequested.compareAndSet(false, true);
    if (!notAlreadyRequested) {
      logger.warn("Shutdown of kubernetes watch was already requested");
      return notAlreadyRequested;
    }
    logger.info("Requesting shutdown for kubernetes watch");
    this.watchRunner.interrupt();
    return notAlreadyRequested;
  }

  /**
   * Parameters used for setting up the Pod watch.
   */
  public static class PodWatchParams {
    private final static int DEFAULT_RESET_DELAY_MILLIS = 10 * 1000;
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
      return this.namespace;
    }

    public String getLabelSelector() {
      return this.labelSelector;
    }

    public int getResetDelayMillis() {
      return this.resetDelayMillis;
    }
  }
}
