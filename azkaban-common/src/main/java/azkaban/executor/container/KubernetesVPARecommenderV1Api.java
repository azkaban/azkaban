package azkaban.executor.container;

import com.google.gson.reflect.TypeToken;
import io.kubernetes.autoscaling.models.V1VerticalPodAutoscaler;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.ApiResponse;
import io.kubernetes.client.openapi.Configuration;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;

public class KubernetesVPARecommenderV1Api {
  private static final String KUBERNETES_GET_VPA_RECOMMENDER_API_FORMAT = "/apis/autoscaling.k8s"
      + ".io/v1/namespaces/%s/verticalpodautoscalers/%s";
  private static final String KUBERNETES_POST_VPA_RECOMMENDER_API_FORMAT = "/apis/autoscaling.k8s"
      + ".io/v1/namespaces/%s/verticalpodautoscalers";
  private ApiClient localVarApiClient;

  public KubernetesVPARecommenderV1Api() {
    this(Configuration.getDefaultApiClient());
  }

  public KubernetesVPARecommenderV1Api(ApiClient apiClient) {
    this.localVarApiClient = apiClient;
  }

  public ApiClient getApiClient() {
    return localVarApiClient;
  }

  public void setApiClient(ApiClient apiClient) {
    this.localVarApiClient = apiClient;
  }

  /**
   * Build call for getNamespacedVPACall
   * @return Call to execute
   */
  private okhttp3.Call getNamespacedVPACall(
      String namespace,
      String vpaObjectName)
      throws ApiException {
    // create path and map variables
    String path = String.format(KUBERNETES_GET_VPA_RECOMMENDER_API_FORMAT, namespace, vpaObjectName);

    String[] authNames = new String[] { "BearerToken" };
    return localVarApiClient.buildCall(
        path,
        "GET",
        new ArrayList<>(),
        new ArrayList<>(),
        null,
        new HashMap<>(),
        new HashMap<>(),
        new HashMap<>(),
        authNames,
        null);
  }

  /**
   * Get VPA recommendation for a given vpa object in namespace
   * @param namespace kubernetes namespace
   * @param vpaObjectName VPA object name
   * @return response in V1VerticalPodAutoscaler format
   */
  public ApiResponse<V1VerticalPodAutoscaler> getNamespacedVPA(
      String namespace,
      String vpaObjectName)
      throws ApiException {
    okhttp3.Call localVarCall = getNamespacedVPACall(namespace, vpaObjectName);
    Type localVarReturnType = new TypeToken<V1VerticalPodAutoscaler>() {}.getType();
    return localVarApiClient.execute(localVarCall, localVarReturnType);
  }

  /**
   * Build call for createNamespacedVPACall
   * @return Call to execute
   */
  private okhttp3.Call createNamespacedVPACall(
      String namespace,
      V1VerticalPodAutoscaler vpaObject)
      throws ApiException {
    // create path and map variables
    String path = String.format(KUBERNETES_POST_VPA_RECOMMENDER_API_FORMAT, namespace);

    String[] authNames = new String[] { "BearerToken" };
    return localVarApiClient.buildCall(
        path,
        "POST",
        new ArrayList<>(),
        new ArrayList<>(),
        vpaObject,
        new HashMap<>(),
        new HashMap<>(),
        new HashMap<>(),
        authNames,
        null);
  }

  /**
   * Get VPA recommendation for a given vpa object in namespace
   * @param namespace kubernetes namespace
   * @param vpaObject VPA object
   * @return response in V1VerticalPodAutoscaler format
   */
  public ApiResponse<V1VerticalPodAutoscaler> createNamespacedVPA(
      String namespace,
      V1VerticalPodAutoscaler vpaObject)
      throws ApiException {
    okhttp3.Call localVarCall = createNamespacedVPACall(namespace, vpaObject);
    Type localVarReturnType = new TypeToken<V1VerticalPodAutoscaler>() {}.getType();
    return localVarApiClient.execute(localVarCall, localVarReturnType);
  }
}
