package azkaban.executor.container;

import static azkaban.Constants.ContainerizedDispatchManagerProperties.KUBERNETES_VPA_MAX_ALLOWED_GET_RECOMMENDATION_TIMEOUT_SEC;
import static azkaban.Constants.ContainerizedDispatchManagerProperties.KUBERNETES_VPA_MAX_ALLOWED_NO_RECOMMENDATION_SINCE_CREATION_SEC;

import azkaban.executor.ExecutorManagerException;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableMap;
import io.kubernetes.autoscaling.models.V1VerticalPodAutoscaler;
import io.kubernetes.autoscaling.models.V1VerticalPodAutoscalerSpec;
import io.kubernetes.autoscaling.models.V1VerticalPodAutoscalerSpecResourcePolicy;
import io.kubernetes.autoscaling.models.V1VerticalPodAutoscalerSpecResourcePolicyContainerPolicies;
import io.kubernetes.autoscaling.models.V1VerticalPodAutoscalerSpecUpdatePolicy;
import io.kubernetes.autoscaling.models.V1VerticalPodAutoscalerSpecUpdatePolicy.UpdateModeEnum;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the implementation for native Kubernetes VPA Recommender:
 * https://github.com/kubernetes/autoscaler/tree/master/vertical-pod-autoscaler.
 * It has implementation for getting CPU and memory recommendation for a given Azkaban flow. For
 * any recommendation query, it will query the Kubernetes VPA object associated with the given
 * Azkaban flow first: if not exist, create one and apply the default request value for the current
 * execution.
 */
@Singleton
public class KubernetesVPARecommender implements VPARecommender {
  private static final Logger logger = LoggerFactory
      .getLogger(KubernetesVPARecommender.class);

  private static final int DEFAULT_MAX_ALLOWED_NO_RECOMMENDATION_SINCE_CREATION_SEC =
      20 * 60;
  private static final int DEFAULT_MAX_ALLOWED_GET_RECOMMENDATION_TIMEOUT_SEC = 120;

  private static final String VPA_API_VERSION = "autoscaling.k8s.io/v1";
  private static final String VPA_KIND = "VerticalPodAutoscaler";

  private static final String VPA_CPU_KEY = "cpu";
  private static final String VPA_MEMORY_KEY = "memory";

  private final int maxAllowedNoRecommendationSinceCreationSec;
  // Complete get recommendation function within this timeout.
  private final int maxAllowedGetRecommendationTimeoutSec;
  private final KubernetesVPARecommenderV1Api kubernetesVPARecommenderV1Api;

  @Inject
  public KubernetesVPARecommender(final Props azkProps, final ApiClient client) {
    this.maxAllowedNoRecommendationSinceCreationSec =
        azkProps.getInt(KUBERNETES_VPA_MAX_ALLOWED_NO_RECOMMENDATION_SINCE_CREATION_SEC,
            DEFAULT_MAX_ALLOWED_NO_RECOMMENDATION_SINCE_CREATION_SEC);
    this.maxAllowedGetRecommendationTimeoutSec =
        azkProps.getInt(KUBERNETES_VPA_MAX_ALLOWED_GET_RECOMMENDATION_TIMEOUT_SEC,
            DEFAULT_MAX_ALLOWED_GET_RECOMMENDATION_TIMEOUT_SEC);
    this.kubernetesVPARecommenderV1Api = new KubernetesVPARecommenderV1Api(client);
  }

  private void createVPAObject(final String namespace, final String flowVPALabelName,
      final String flowVPAName, final String flowContainerName,
      final String minAllowedCPU, final String minAllowedMemory, final String maxAllowedCPU,
      final String maxAllowedMemory) throws ExecutorManagerException {
    try {
      final V1VerticalPodAutoscaler vpaObject = new V1VerticalPodAutoscaler()
          .apiVersion(VPA_API_VERSION)
          .kind(VPA_KIND)
          .metadata(new V1ObjectMeta().name(flowVPAName))
          .spec(new V1VerticalPodAutoscalerSpec()
              .selector(new V1LabelSelector().matchLabels(Collections.singletonMap(flowVPALabelName,
                  flowVPAName)))
              .resourcePolicy(new V1VerticalPodAutoscalerSpecResourcePolicy()
                  .containerPolicies(
                      Collections.singletonList(new V1VerticalPodAutoscalerSpecResourcePolicyContainerPolicies()
                          .containerName(flowContainerName)
                          .minAllowed(ImmutableMap
                              .of(VPA_CPU_KEY, minAllowedCPU, VPA_MEMORY_KEY, minAllowedMemory))
                          .maxAllowed(ImmutableMap
                              .of(VPA_CPU_KEY, maxAllowedCPU, VPA_MEMORY_KEY, maxAllowedMemory))
                      ))
              )
              .updatePolicy(new V1VerticalPodAutoscalerSpecUpdatePolicy().updateMode(UpdateModeEnum.OFF))
          );

      kubernetesVPARecommenderV1Api.createNamespacedVPA(namespace, vpaObject);
    } catch (Exception e) {
      throw new ExecutorManagerException(e);
    }
  }

  @Override
  public VPARecommendation getFlowContainerRecommendedRequests(final String namespace,
      final String flowVPALabelName, final String flowVPAName,
      final String flowContainerName, final double cpuRecommendationMultiplier,
      final double memoryRecommendationMultiplier,
      final String minAllowedCPU, final String minAllowedMemory, final String maxAllowedCPU,
      final String maxAllowedMemory) throws ExecutorManagerException {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<VPARecommendation> handler = executor.submit(() -> {
        final V1VerticalPodAutoscaler vpaObject;
        try {
          vpaObject = kubernetesVPARecommenderV1Api.getNamespacedVPA(namespace, flowVPAName).getData();
        } catch (ApiException e) {
          // If VPA object not found: should create and then return null.
          if (e.getCode() == HttpStatus.SC_NOT_FOUND) {
            KubernetesVPARecommender.this.createVPAObject(namespace, flowVPALabelName,
                flowVPAName,
                flowContainerName,
                minAllowedCPU, minAllowedMemory, maxAllowedCPU, maxAllowedMemory);
            return null;
          }

          // Unknown errors
          throw e;
        }

        // If VPA object is newly created: no recommendation yet and should return null.
        if (vpaObject.getMetadata() == null || vpaObject.getMetadata().getCreationTimestamp() == null ||
            Duration.between(vpaObject.getMetadata().getCreationTimestamp(), OffsetDateTime.now())
                .compareTo(Duration.ofSeconds(KubernetesVPARecommender.this.maxAllowedNoRecommendationSinceCreationSec)) < 0) {
          return null;
        }

        try {
          final Map<String, Object> recommendation =
              vpaObject.getStatus().getRecommendation().getContainerRecommendations()
                  .stream()
                  .filter(r -> flowContainerName.equals(r.getContainerName()))
                  .findFirst()
                  .get()
                  .getTarget();

          final Quantity rawCpuRecommendationQuantity =
              new Quantity(recommendation.get(VPA_CPU_KEY).toString());
          final Quantity rawMemoryRecommendationQuantity =
              new Quantity(recommendation.get(VPA_MEMORY_KEY).toString());

          // Kubernetes doesn't allow you to specify CPU resources with a precision finer than 1m
          final String cpuRecommendationMilliUnit =
              rawCpuRecommendationQuantity.getNumber().multiply(new BigDecimal(cpuRecommendationMultiplier * 1000)).toBigInteger().toString();
          // Better to specify memory resources with a precision as fine as 1 unit
          final String memoryRecommendationUnit =
              rawMemoryRecommendationQuantity.getNumber().multiply(new BigDecimal(memoryRecommendationMultiplier)).toBigInteger().toString();

          final Quantity cpuRecommendationQuantity = new Quantity(cpuRecommendationMilliUnit + "m");
          final Quantity memoryRecommendationQuantity = new Quantity(memoryRecommendationUnit);

          logger.info(String.format("Raw recommendation quantities for %s: %s, %s", flowVPAName,
              rawCpuRecommendationQuantity.toSuffixedString(),
              rawMemoryRecommendationQuantity.toSuffixedString()));

          logger.info(String.format("Multiplied recommendation quantities for %s: %s, %s",
              flowVPAName,
              cpuRecommendationQuantity.toSuffixedString(),
              memoryRecommendationQuantity.toSuffixedString()));

          return new VPARecommendation(cpuRecommendationQuantity.toSuffixedString(),
              memoryRecommendationQuantity.toSuffixedString());
        } catch (NullPointerException | NoSuchElementException e) {
          logger.warn("VPA object for flowVPAName " + flowVPAName + " is found but recommendation has "
              + "not been generated yet", e);

          // Unknown errors
          throw e;
        }
      });

      return handler.get(maxAllowedGetRecommendationTimeoutSec, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new ExecutorManagerException(e);
    } finally {
      executor.shutdownNow();
    }
  }
}
