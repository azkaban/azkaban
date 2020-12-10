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

import azkaban.Constants.ConfigurationKeys;
import azkaban.Constants.ContainerizedExecutionManagerProperties;
import azkaban.container.models.AzKubernetesV1PodBuilder;
import azkaban.container.models.AzKubernetesV1SpecBuilder;
import azkaban.container.models.ImagePullPolicy;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableMap;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1DeleteOptions;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Status;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Yaml;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is Kubernetes based implementation for containerization. It has implementation for
 * creation/deletion of Pod and service. For any execution, it will identify version set and create
 * a pod for all the valid jobTypes of a flow.
 */
@Singleton
public class KubernetesContainerizedImpl implements ContainerizedImpl {

  public static final String DEFAULT_FLOW_CONTAINER_NAME_PREFIX = "az-flow-container";
  public static final String DEFAULT_POD_NAME_PREFIX = "fc-dep";
  public static final String DEFAULT_SERVICE_NAME_PREFIX = "fc-svc";
  public static final String DEFAULT_CLUSTER_NAME = "azkaban";
  public static final String CPU_LIMIT = "4";
  public static final String DEFAULT_CPU_REQUEST = "1";
  public static final String MEMORY_LIMIT = "64Gi";
  public static final String DEFAULT_MEMORY_REQUEST = "2Gi";

  private final String namespace;
  private final ApiClient client;
  private final CoreV1Api coreV1Api;
  private final Props azkProps;
  private final ExecutorLoader executorLoader;
  private final String podPrefix;
  private final String servicePrefix;
  private final String clusterName;
  private final String flowContainerName;
  private final String cpuLimit;
  private final String cpuRequest;
  private final String memoryLimit;
  private final String memoryRequest;


  private static final Logger logger = LoggerFactory
      .getLogger(KubernetesContainerizedImpl.class);

  @Inject
  public KubernetesContainerizedImpl(final Props azkProps, final ExecutorLoader executorLoader)
      throws ExecutorManagerException {
    this.azkProps = azkProps;
    this.executorLoader = executorLoader;
    this.namespace = this.azkProps
        .getString(ContainerizedExecutionManagerProperties.KUBERNETES_NAMESPACE);
    this.flowContainerName =
        this.azkProps.getString(ContainerizedExecutionManagerProperties.KUBERNETES_FLOW_CONTAINER_NAME
            , DEFAULT_FLOW_CONTAINER_NAME_PREFIX);
    this.podPrefix =
        this.azkProps.getString(ContainerizedExecutionManagerProperties.KUBERNETES_POD_NAME_PREFIX,
            DEFAULT_POD_NAME_PREFIX);
    this.servicePrefix = this.azkProps
        .getString(ContainerizedExecutionManagerProperties.KUBERNETES_SERVICE_NAME_PREFIX,
            DEFAULT_SERVICE_NAME_PREFIX);
    this.clusterName = this.azkProps.getString(ConfigurationKeys.AZKABAN_CLUSTER_NAME,
        DEFAULT_CLUSTER_NAME);
    this.cpuLimit = this.azkProps
        .getString(ContainerizedExecutionManagerProperties.KUBERNETES_FLOW_CONTAINER_CPU_LIMIT,
            CPU_LIMIT);
    this.cpuRequest = this.azkProps
        .getString(ContainerizedExecutionManagerProperties.KUBERNETES_FLOW_CONTAINER_CPU_REQUEST,
            DEFAULT_CPU_REQUEST);
    this.memoryLimit = this.azkProps
        .getString(ContainerizedExecutionManagerProperties.KUBERNETES_FLOW_CONTAINER_MEMORY_LIMIT,
            MEMORY_LIMIT);
    this.memoryRequest = this.azkProps
        .getString(ContainerizedExecutionManagerProperties.KUBERNETES_FLOW_CONTAINER_MEMORY_REQUEST,
            DEFAULT_MEMORY_REQUEST);

    try {
      // Path to the configuration file for Kubernetes which contains information about
      // Kubernetes API Server and identity for authentication
      final String kubeConfigPath = this.azkProps
          .getString(ContainerizedExecutionManagerProperties.KUBERNETES_KUBE_CONFIG_PATH);
      logger.info("Kube config path is : {}", kubeConfigPath);
      this.client =
          ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(
              Files.newBufferedReader(Paths.get(kubeConfigPath), Charset.defaultCharset())))
              .build();
      this.coreV1Api = new CoreV1Api(this.client);
    } catch (final IOException exception) {
      logger.error("Unable to read kube config file: {}", exception.getMessage());
      throw new ExecutorManagerException(exception);
    }
  }

  /**
   * This method is used to create container during dispatch of execution. It will create pod for a
   * flow execution. It will also create a service for a pod if azkaban.kubernetes.service .required
   * property is set.
   *
   * @param executionId
   * @throws ExecutorManagerException
   */
  @Override
  public void createContainer(final int executionId) throws ExecutorManagerException {
    createPod(executionId);
    if (isServiceRequired()) {
      createService(executionId);
    }
  }

  /**
   * This method is used to delete container. It will delete pod for a flow execution. If the
   * service was created then it will also delete the service. This method can be called as a part
   * of cleanup process for containers in case containers didn't shutdown gracefully.
   *
   * @param executionId
   * @throws ExecutorManagerException
   */
  @Override
  public void deleteContainer(final int executionId) throws ExecutorManagerException {
    deletePod(executionId);
    if (isServiceRequired()) {
      deleteService(executionId);
    }
  }

  /**
   * This method is used to create pod. 1. Fetch jobTypes for the flow 2. Fetch flow parameters for
   * version set and each image type if it is set. 3. If valid version set is provided then use
   * versions from it. 4. If valid version set is not provided then call Ramp up manager API and get
   * image version for each image type. 5. Add all the validation around a) whether version set is
   * valid or not. b) If it is valid then is there any change in flow and new jobType is introduced
   * after version set was created? If so, create new version set using versions mentioned in
   * version set and ramp up for new jobType. 6. Create pod spec using all the version information
   * 7. Insert version set into execution_flows tables for a reference 8. Emit version set as a part
   * of flow life cycle event.
   *
   * @param executionId
   * @throws ExecutorManagerException
   */
  private void createPod(final int executionId) throws ExecutorManagerException {
    // Fetch execution flow from execution Id.
    final ExecutableFlow flow = this.executorLoader.fetchExecutableFlow(executionId);
    // Step 1: Fetch set of jobTypes for a flow from executionId
    final TreeSet<String> jobTypes = getJobTypesForFlow(flow);
    logger.info("Jobtypes for flow {} are: {}", flow.getFlowId(), jobTypes);

    // TODO: From Flow Param, check if versionSet Num is mentioned, then pick version Set
    //  JSON from version set table using DAO if not then call below mentioned ramp up
    //  API to get version information for each jobtype
    final Map<String, String> flowParam =
        flow.getExecutionOptions().getFlowParameters();

    // TODO: Validation for valid version number ->
    //  1. Version is in image version's table no matter what it's status is
    //  2.  Version set number: 155 , Create a new version set using version set mentioned in
    //   number and for new jobtype, whatever active version is there. Add descriptive message
    //   for which jobtype is missing in version set number and create new one and mention which
    //   version you are picking for new jobtypes. Version set number and json can be set in
    //   environment variable and Flow container can read it and add it in log
    //  3. Add placeholder for verification whether version is available in Artifactory (Is
    //   there a way to capture events from version release or deprecation in artifactory that we
    //   can consume and use here)
    if (flowParam != null && !flowParam.isEmpty()) {
      logger.info("Flow Parameters are: " + flowParam);
    }

    // TODO: Populating version set -> What is there in database on top of that what is passed in
    //   flow parameters (Read it from cache. Write to both for db and cache in case of change)
    // TODO: Below mentioned flow container image and conf version should come from database.
    final String azkabanBaseImageVersion = getAzkabanBaseImageVersion();
    final String azkabanConfigVersion = getAzkabanConfigVersion();

    final AzKubernetesV1SpecBuilder v1SpecBuilder = new AzKubernetesV1SpecBuilder(this.clusterName,
        Optional.empty())
        .addFlowContainer(this.flowContainerName, azkabanBaseImageVersion,
            ImagePullPolicy.IF_NOT_PRESENT,
            azkabanConfigVersion)
        .withResources(this.cpuLimit, this.cpuRequest, this.memoryLimit, this.memoryRequest);

    // Create init container yaml file for each jobType
    addInitContainerForAllJobTypes(executionId, jobTypes, v1SpecBuilder);

    final V1PodSpec podSpec = v1SpecBuilder.build();

    final ImmutableMap<String, String> labels = getLabelsForPod();
    final ImmutableMap<String, String> annotations = getAnnotationsForPod();

    final V1Pod pod = new AzKubernetesV1PodBuilder(getPodName(executionId), this.namespace, podSpec)
        .withPodLabels(labels)
        .withPodAnnotations(annotations)
        .build();

    final String createdPodSpec = Yaml.dump(pod).trim();
    logger.debug("Pod spec for execution id {} is {}", executionId, createdPodSpec);
    // TODO: Call the API to create version set for this execution if it does not exist. Make
    //  sure to pass tree map to maintain order so that md5 won't change.

    // TODO: Add version set number and json in flow life cycle event so users can use this
    //   information
    try {
      this.coreV1Api.createNamespacedPod(this.namespace, pod, null, null, null);
    } catch (ApiException e) {
      logger.error("Unable to create StatefulSet: {}", e.getMessage());
      throw new ExecutorManagerException(e);
    }

    // TODO: Store version set id in execution_flows for execution_id
  }

  /**
   * TODO: Get azkaban base image version from version set.
   *
   * @return
   */
  private String getAzkabanBaseImageVersion() {
    return null;
  }

  private String getAzkabanConfigVersion() {
    return null;
  }

  /**
   * TODO: Add implementation to get labels for Pod.
   *
   * @return
   */
  private ImmutableMap getLabelsForPod() {
    return ImmutableMap.of("cluster", this.clusterName);
  }

  /**
   * TODO: Add implementation to get annotations for Pod.
   *
   * @return
   */
  private ImmutableMap getAnnotationsForPod() {
    return ImmutableMap.of();
  }

  /**
   * TODO: Add implementation for this method as mentioned over here. 1. Call Ramp up manager to get
   * version number for each jobtype. Accept lower case for image type name. For version set json,
   * use lower case.
   *
   * @param executionId
   * @param jobTypes
   * @param v1SpecBuilder
   * @throws ExecutorManagerException
   */
  private void addInitContainerForAllJobTypes(final int executionId,
      final Set<String> jobTypes, final AzKubernetesV1SpecBuilder v1SpecBuilder)
      throws ExecutorManagerException {

  }

  /**
   * This method is used to get jobTypes for a flow. This method is going to call
   * populateJobTypeForFlow which has recursive method call to traverse the DAG for a flow.
   *
   * @param flow Executable flow object
   * @return
   * @throws ExecutorManagerException
   */
  public TreeSet<String> getJobTypesForFlow(final ExecutableFlow flow) {
    final TreeSet<String> jobTypes = new TreeSet<>();
    populateJobTypeForFlow(flow, jobTypes);
    return jobTypes;
  }

  /**
   * This method is used to populate jobTypes for ExecutableNode.
   *
   * @param node
   * @param jobTypes
   */
  private void populateJobTypeForFlow(final ExecutableNode node, Set<String> jobTypes) {
    if (node instanceof ExecutableFlowBase) {
      final ExecutableFlowBase base = (ExecutableFlowBase) node;
      for (ExecutableNode subNode : base.getExecutableNodes()) {
        populateJobTypeForFlow(subNode, jobTypes);
      }
    } else {
      jobTypes.add(node.getType());
    }
  }

  /**
   * This method is used to create service for flow container for execution id.
   *
   * @param executionId
   * @throws ExecutorManagerException
   */
  private void createService(final int executionId) throws ExecutorManagerException {
    // TODO: Add integration with Service spec builder once that code is available in master branch
  }

  /**
   * This method is used to check whether service should be created in Kubernetes for flow container
   * pod or not.
   *
   * @return
   */
  private boolean isServiceRequired() {
    return this.azkProps
        .getBoolean(ContainerizedExecutionManagerProperties.KUBERNETES_SERVICE_REQUIRED, false);
  }

  /**
   * This method is used to delete pod in Kubernetes. It will terminate the pod. deployment is
   * fixed
   *
   * @param executionId
   * @throws ExecutorManagerException
   */
  private void deletePod(final int executionId) throws ExecutorManagerException {
    try {
      final String podName = getPodName(executionId);
      this.coreV1Api.deleteNamespacedPod(podName, this.namespace, null, null,
          null, null, null, new V1DeleteOptions());
      logger.info("Action: Pod Deletion, Pod Name: {}", podName);
    } catch (ApiException e) {
      logger.error("Unable to delete Pod in Kubernetes: {}", e.getMessage());
      throw new ExecutorManagerException(e);
    }
  }

  /**
   * This method is used to delete service in Kubernetes which is created for Pod.
   *
   * @param executionId
   * @throws ExecutorManagerException
   */
  public void deleteService(final int executionId) throws ExecutorManagerException {
    final String serviceName = getServiceName(executionId);
    try {
      final V1Status deleteResult = this.coreV1Api.deleteNamespacedService(
          serviceName,
          this.namespace,
          null,
          null,
          null,
          null,
          null,
          new V1DeleteOptions());
      logger.info("Action: Service Deletion, Service Name: {}, code: {}, message: {}",
          serviceName,
          deleteResult.getCode(),
          deleteResult.getMessage());
    } catch (ApiException e) {
      logger.error("Unable to delete service in Kubernetes: {}", e.getMessage());
      throw new ExecutorManagerException(e);
    }
  }

  /**
   * This method is used to get service name. It will be created using service name prefix, azkaban
   * cluster name and execution id.
   *
   * @param executionId
   * @return
   */
  private String getServiceName(final int executionId) {
    return String.join("-", this.servicePrefix, this.clusterName, String.valueOf(executionId));
  }

  /**
   * This method is used to get name of Pod based on naming convention. It will be created using pod
   * name prefix, azkaban cluster name and execution id.
   *
   * @param executionId
   * @return
   */
  private String getPodName(final int executionId) {
    return String.join("-", this.podPrefix, this.clusterName, String.valueOf(executionId));
  }
}
