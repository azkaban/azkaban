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

import static azkaban.executor.container.KubernetesContainerizedImpl.CLUSTER_LABEL_NAME;
import static azkaban.executor.container.KubernetesContainerizedImpl.EXECUTION_ID_LABEL_NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is intended for maintaining any relevant data from a Pod Watch event that was derived
 * during the inference of {@link AzPodStatus}. The data here can be consumed by any downstream
 * callbacks.
 */
public class AzPodStatusMetadata {
  private static final Logger logger = LoggerFactory.getLogger(AzPodStatusMetadata.class);

  private final AzPodStatus azPodStatus;
  private final String podName;
  private final Watch.Response<V1Pod> podWatchEvent;
  private final Optional<FlowPodMetadata> flowPodMetadata;

  public AzPodStatusMetadata(AzPodStatusExtractor extractor) {
    this.azPodStatus = extractor.createAzPodStatus();
    this.podName = extractor.getPodName();
    this.podWatchEvent = extractor.getPodWatchEvent();
    this.flowPodMetadata = FlowPodMetadata.extract(extractor);
  }

  public AzPodStatus getAzPodStatus() {
    return azPodStatus;
  }

  public String getPodName() {
    return podName;
  }

  public Watch.Response<V1Pod> getPodWatchEvent() {
    return podWatchEvent;
  }

  public Optional<FlowPodMetadata> getFlowPodMetadata() {
    return flowPodMetadata;
  }

  /**
   * This is specifically for maintaining data relevant to a FlowContainer pod. Any data not
   * specific to FlowContainers should be directly added to the outer class {@link
   * AzPodStatusMetadata}
   */
  public static class FlowPodMetadata {
    private final String executionId;
    private final String clusterName;

    private FlowPodMetadata(String executionId, String clusterName) {
      this.executionId = executionId;
      this.clusterName = clusterName;
    }

    public String getExecutionId() {
      return executionId;
    }

    public String getClusterName() {
      return clusterName;
    }

    public static Optional<FlowPodMetadata> extract(AzPodStatusExtractor podStatusExtractor) {
      requireNonNull(podStatusExtractor.getV1Pod(), "pod must not be null");
      requireNonNull(podStatusExtractor.getV1Pod().getMetadata(), "pod metadata must not be null");
      requireNonNull(podStatusExtractor.getV1Pod().getMetadata().getName(), "pod name must not be null");

      String podName = podStatusExtractor.getV1Pod().getMetadata().getName();
      String executionId = null;
      String clusterName = null;

      if (podStatusExtractor.getV1Pod().getMetadata().getLabels() == null) {
        logger.warn("No labels found for pod: " + podName);
        return Optional.empty();
      }
      clusterName = podStatusExtractor.getV1Pod().getMetadata().getLabels().get(CLUSTER_LABEL_NAME);
      if (clusterName == null) {
        logger.warn(format("Label %s not found for pod %s", CLUSTER_LABEL_NAME, podName));
        return Optional.empty();
      }
      executionId = podStatusExtractor.getV1Pod().getMetadata().getLabels().get(EXECUTION_ID_LABEL_NAME);
      if (executionId == null) {
        logger.warn(format("Label %s not found for pod %s", EXECUTION_ID_LABEL_NAME, podName));
        return Optional.empty();
      }
      return Optional.of(new FlowPodMetadata(executionId, clusterName));
    }
  }
}
