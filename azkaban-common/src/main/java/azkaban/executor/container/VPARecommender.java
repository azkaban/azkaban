package azkaban.executor.container;

import azkaban.executor.ExecutorManagerException;
import javax.annotation.Nullable;

public interface VPARecommender {
  /**
   * Get recommended flow container resource requests from VPA recommender. If not available, fall
   * back to the max allowed resources for top-down optimal resource utilization searches.
   *
   * @param namespace Kubernetes namespace
   * @param flowVPALabelName Label name for the VPA object associated with the Azkaban flow
   * @param flowVPAName Name for the VPA object associated with the Azkaban flow
   * @param flowContainerName flow container name to provide recommendation
   * @param cpuRecommendationMultiplier CPU recommendation multiplier
   * @param memoryRecommendationMultiplier memory recommendation multiplier
   * @param minAllowedCPU minimum allowed CPU before applying multiplier
   * @param minAllowedMemory minimum allowed memory before applying multiplier
   * @param maxAllowedCPU maximum allowed CPU before applying multiplier
   * @param maxAllowedMemory maximum allowed memory before applying multiplier
   * @return Recommended resource requests for a flow container or null for recommendation not
   * ready yet
   */
  @Nullable
  VPARecommendation getFlowContainerRecommendedRequests(String namespace,
      String flowVPALabelName,
      String flowVPAName, String flowContainerName,
      double cpuRecommendationMultiplier, double memoryRecommendationMultiplier,
      String minAllowedCPU, String minAllowedMemory,
      String maxAllowedCPU, String maxAllowedMemory) throws ExecutorManagerException;
}
