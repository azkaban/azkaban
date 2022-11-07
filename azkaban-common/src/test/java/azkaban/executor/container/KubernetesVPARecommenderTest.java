package azkaban.executor.container;

import static azkaban.Constants.ContainerizedDispatchManagerProperties.KUBERNETES_VPA_MAX_ALLOWED_GET_RECOMMENDATION_TIMEOUT_SEC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutorManagerException;
import azkaban.metrics.ContainerizationMetrics;
import azkaban.metrics.DummyContainerizationMetricsImpl;
import azkaban.utils.Props;
import io.kubernetes.autoscaling.models.V1VerticalPodAutoscaler;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.ApiResponse;
import io.kubernetes.client.util.Yaml;
import java.io.File;
import java.time.OffsetDateTime;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class KubernetesVPARecommenderTest {

  private static final String KUBERNETES_NAMESPACE = "default";
  private static final String FLOW_VPA_LABEL_NAME = "flow-vpa";
  private static final String FLOW_VPA_NAME = "fc-vpa-azkaban-123";
  private static final String FLOW_CONTAINER_NAME = "az-platform-image";
  private static final double CPU_RECOMMENDATION_MULTIPLIER = 1.25;
  private static final double MEMORY_RECOMMENDATION_MULTIPLIER = 1.25;
  private static final String MIN_ALLOWED_CPU = "500m";
  private static final String MIN_ALLOWED_MEMORY = "4Gi";
  private static final String MAX_ALLOWED_CPU = "8";
  private static final String MAX_ALLOWED_MEMORY = "64Gi";
  private static final String EXPECTED_CPU_RECOMMENDATION_AFTER_MULTIPLIER = "1250m";
  private static final String EXPECTED_MEMORY_RECOMMENDATION_AFTER_MULTIPLIER = "10737418240";
  private ContainerizationMetrics containerizationMetrics;
  private KubernetesVPARecommender kubernetesVPARecommender;
  private Props props = new Props();
  private ApiClient apiClient;
  private V1VerticalPodAutoscaler vpaWithoutRecommendationRecent;
  private V1VerticalPodAutoscaler vpaWithoutRecommendationOld;
  private V1VerticalPodAutoscaler vpaWithRecommendationRecent;
  private V1VerticalPodAutoscaler vpaWithRecommendationOld;

  @Before
  public void setUp() throws Exception {
    final ClassLoader currentClassLoader = getClass().getClassLoader();
    // Set higher timeout in unit tests in case of flaky tests
    this.props.put(KUBERNETES_VPA_MAX_ALLOWED_GET_RECOMMENDATION_TIMEOUT_SEC, 60 * 60);
    this.apiClient = mock(ApiClient.class);
    this.containerizationMetrics = new DummyContainerizationMetricsImpl();
    this.kubernetesVPARecommender = new KubernetesVPARecommender(this.props, this.apiClient, this.containerizationMetrics);
    this.vpaWithoutRecommendationRecent = Yaml.loadAs(new File(currentClassLoader.getResource(
            "vpa_without_recommendation.yaml").getFile()), V1VerticalPodAutoscaler.class);
    this.vpaWithoutRecommendationRecent.setMetadata(this.vpaWithoutRecommendationRecent.getMetadata().creationTimestamp(
        OffsetDateTime.now().minusSeconds(1)));
    this.vpaWithoutRecommendationOld = Yaml.loadAs(new File(currentClassLoader.getResource(
        "vpa_without_recommendation.yaml").getFile()), V1VerticalPodAutoscaler.class);
    this.vpaWithoutRecommendationOld.setMetadata(this.vpaWithoutRecommendationOld.getMetadata().creationTimestamp(
        OffsetDateTime.now().minusYears(1)));
    this.vpaWithRecommendationRecent = Yaml.loadAs(new File(currentClassLoader.getResource(
        "vpa_with_recommendation.yaml").getFile()), V1VerticalPodAutoscaler.class);
    this.vpaWithRecommendationRecent.setMetadata(this.vpaWithRecommendationRecent.getMetadata().creationTimestamp(
        OffsetDateTime.now().minusSeconds(1)));
    this.vpaWithRecommendationOld = Yaml.loadAs(new File(currentClassLoader.getResource(
        "vpa_with_recommendation.yaml").getFile()), V1VerticalPodAutoscaler.class);
    this.vpaWithRecommendationOld.setMetadata(this.vpaWithRecommendationOld.getMetadata().creationTimestamp(
        OffsetDateTime.now().minusYears(1)));
  }

  @Test
  public void createVPAObjectIfNotFound() throws Exception {
    when(this.apiClient.execute(any(), any())).thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND,
        "")).thenReturn(new ApiResponse(HttpStatus.SC_CREATED, null,
        this.vpaWithRecommendationOld));

    VPARecommendation vpaRecommendation =
        kubernetesVPARecommender.getFlowContainerRecommendedRequests(KUBERNETES_NAMESPACE,
        FLOW_VPA_LABEL_NAME, FLOW_VPA_NAME, FLOW_CONTAINER_NAME, CPU_RECOMMENDATION_MULTIPLIER,
        MEMORY_RECOMMENDATION_MULTIPLIER, MIN_ALLOWED_CPU, MIN_ALLOWED_MEMORY, MAX_ALLOWED_CPU,
        MAX_ALLOWED_MEMORY);

    Assert.assertNull(vpaRecommendation);
    verify(this.apiClient, Mockito.times(2)).execute(any(), any());
  }

  @Test(expected = ExecutorManagerException.class)
  public void throwExceptionIfNoRecommendationForOldVPAObject() throws Exception {
    when(this.apiClient.execute(any(), any())).thenReturn(new ApiResponse(HttpStatus.SC_OK, null,
        this.vpaWithoutRecommendationOld));

    kubernetesVPARecommender.getFlowContainerRecommendedRequests(KUBERNETES_NAMESPACE,
        FLOW_VPA_LABEL_NAME, FLOW_VPA_NAME, FLOW_CONTAINER_NAME, CPU_RECOMMENDATION_MULTIPLIER,
        MEMORY_RECOMMENDATION_MULTIPLIER, MIN_ALLOWED_CPU, MIN_ALLOWED_MEMORY, MAX_ALLOWED_CPU,
        MAX_ALLOWED_MEMORY);
  }

  @Test
  public void returnNullIfForRecentVPAObjectWithoutRecommendation() throws Exception {
    when(this.apiClient.execute(any(), any())).thenReturn(new ApiResponse(HttpStatus.SC_OK, null,
        this.vpaWithoutRecommendationRecent));

    VPARecommendation vpaRecommendation =
        kubernetesVPARecommender.getFlowContainerRecommendedRequests(KUBERNETES_NAMESPACE,
            FLOW_VPA_LABEL_NAME, FLOW_VPA_NAME, FLOW_CONTAINER_NAME, CPU_RECOMMENDATION_MULTIPLIER,
            MEMORY_RECOMMENDATION_MULTIPLIER, MIN_ALLOWED_CPU, MIN_ALLOWED_MEMORY, MAX_ALLOWED_CPU,
            MAX_ALLOWED_MEMORY);

    Assert.assertNull(vpaRecommendation);
    verify(this.apiClient, Mockito.times(1)).execute(any(), any());
  }

  @Test
  public void returnNullIfForRecentVPAObjectWithRecommendation() throws Exception {
    when(this.apiClient.execute(any(), any())).thenReturn(new ApiResponse(HttpStatus.SC_OK, null,
        this.vpaWithRecommendationRecent));

    VPARecommendation vpaRecommendation =
        kubernetesVPARecommender.getFlowContainerRecommendedRequests(KUBERNETES_NAMESPACE,
            FLOW_VPA_LABEL_NAME, FLOW_VPA_NAME, FLOW_CONTAINER_NAME, CPU_RECOMMENDATION_MULTIPLIER,
            MEMORY_RECOMMENDATION_MULTIPLIER, MIN_ALLOWED_CPU, MIN_ALLOWED_MEMORY, MAX_ALLOWED_CPU,
            MAX_ALLOWED_MEMORY);

    Assert.assertNull(vpaRecommendation);
    verify(this.apiClient, Mockito.times(1)).execute(any(), any());
  }

  @Test
  public void applyMultiplier() throws Exception {
    when(this.apiClient.execute(any(), any())).thenReturn(new ApiResponse(HttpStatus.SC_OK, null,
        this.vpaWithRecommendationOld));

    VPARecommendation vpaRecommendation =
        kubernetesVPARecommender.getFlowContainerRecommendedRequests(KUBERNETES_NAMESPACE,
            FLOW_VPA_LABEL_NAME, FLOW_VPA_NAME, FLOW_CONTAINER_NAME, CPU_RECOMMENDATION_MULTIPLIER,
            MEMORY_RECOMMENDATION_MULTIPLIER, MIN_ALLOWED_CPU, MIN_ALLOWED_MEMORY, MAX_ALLOWED_CPU,
            MAX_ALLOWED_MEMORY);

    Assert.assertEquals(vpaRecommendation.getCpuRecommendation(), EXPECTED_CPU_RECOMMENDATION_AFTER_MULTIPLIER);
    Assert.assertEquals(vpaRecommendation.getMemoryRecommendation(), EXPECTED_MEMORY_RECOMMENDATION_AFTER_MULTIPLIER);
    verify(this.apiClient, Mockito.times(1)).execute(any(), any());
  }

  @Test(expected = ExecutorManagerException.class)
  public void throwTimeoutExceptionIfProcessTakesTooLong() throws Exception {
    // Trigger timeout sooner
    this.props.put(KUBERNETES_VPA_MAX_ALLOWED_GET_RECOMMENDATION_TIMEOUT_SEC, 30);
    this.kubernetesVPARecommender = new KubernetesVPARecommender(this.props, this.apiClient, this.containerizationMetrics);

    when(this.apiClient.execute(any(), any())).thenAnswer(i -> {
      Thread.sleep(3 * 60 * 1000);
      return new ApiResponse<>(HttpStatus.SC_OK, null,
          this.vpaWithRecommendationOld);
    });

    kubernetesVPARecommender.getFlowContainerRecommendedRequests(KUBERNETES_NAMESPACE,
        FLOW_VPA_LABEL_NAME, FLOW_VPA_NAME, FLOW_CONTAINER_NAME, CPU_RECOMMENDATION_MULTIPLIER,
        MEMORY_RECOMMENDATION_MULTIPLIER, MIN_ALLOWED_CPU, MIN_ALLOWED_MEMORY, MAX_ALLOWED_CPU,
        MAX_ALLOWED_MEMORY);
  }
}
