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

import static azkaban.Constants.ImageMgmtConstants.AZKABAN_CONFIG;
import static azkaban.Constants.ImageMgmtConstants.AZKABAN_BASE_IMAGE;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.container.models.AzKubernetesV1PodBuilder;
import azkaban.container.models.AzKubernetesV1ServiceBuilder;
import azkaban.container.models.AzKubernetesV1SpecBuilder;
import azkaban.container.models.ImagePullPolicy;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.imagemgmt.rampup.ImageRampupManager;
import azkaban.imagemgmt.version.VersionSet;
import azkaban.imagemgmt.version.VersionSetBuilder;
import azkaban.imagemgmt.version.VersionSetLoader;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableMap;
import com.google.common.annotations.VisibleForTesting;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1DeleteOptions;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1Status;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Yaml;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
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
  public static final String MAPPING = "Mapping";
  public static final String SERVICE_API_VERSION_2 = "ambassador/v2";
  public static final String DEFAULT_INIT_MOUNT_PATH_PREFIX_FOR_JOBTYPES = "/data/jobtypes";
  public static final String DEFAULT_APP_MOUNT_PATH_PREFIX_FOR_JOBTYPES =
      "/export/apps/azkaban/azkaban-exec-server/current/plugins/jobtypes";
  public static final String IMAGE = "image";
  public static final String VERSION = "version";
  public static final String NSCD_SOCKET_VOLUME_NAME = "nscd-socket";
  public static final String DEFAULT_NSCD_SOCKET_HOST_PATH = "/var/run/nscd/socket";
  public static final String HOST_PATH_TYPE = "Socket";
  public static final String DEFAULT_NSCD_SOCKET_VOLUME_MOUNT_PATH = "/var/run/nscd/socket";
  private static final String NOOP_TYPE = "noop";

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
  private final int servicePort;
  private final long serviceTimeout;
  private final String nscdSocketHostPath;
  private final String nscdSocketVolumeMountPath;
  private final VersionSetLoader versionSetLoader;
  private final ImageRampupManager imageRampupManager;
  private final String initMountPathPrefixForJobtypes;
  private final String appMountPathPrefixForJobtypes;

  private static final Logger logger = LoggerFactory
      .getLogger(KubernetesContainerizedImpl.class);

  @Inject
  public KubernetesContainerizedImpl(final Props azkProps,
      final ExecutorLoader executorLoader,
      final VersionSetLoader versionSetLoader,
      final ImageRampupManager imageRampupManager)
      throws ExecutorManagerException {
    this.azkProps = azkProps;
    this.executorLoader = executorLoader;
    this.versionSetLoader = versionSetLoader;
    this.imageRampupManager = imageRampupManager;
    this.namespace = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_NAMESPACE);
    this.flowContainerName =
        this.azkProps
            .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_NAME
                , DEFAULT_FLOW_CONTAINER_NAME_PREFIX);
    this.podPrefix =
        this.azkProps.getString(ContainerizedDispatchManagerProperties.KUBERNETES_POD_NAME_PREFIX,
            DEFAULT_POD_NAME_PREFIX);
    this.servicePrefix = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_SERVICE_NAME_PREFIX,
            DEFAULT_SERVICE_NAME_PREFIX);
    this.clusterName = this.azkProps.getString(ConfigurationKeys.AZKABAN_CLUSTER_NAME,
        DEFAULT_CLUSTER_NAME);
    this.cpuLimit = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_CPU_LIMIT,
            CPU_LIMIT);
    this.cpuRequest = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_CPU_REQUEST,
            DEFAULT_CPU_REQUEST);
    this.memoryLimit = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_MEMORY_LIMIT,
            MEMORY_LIMIT);
    this.memoryRequest = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_MEMORY_REQUEST,
            DEFAULT_MEMORY_REQUEST);
    this.servicePort =
        this.azkProps.getInt(ContainerizedDispatchManagerProperties.KUBERNETES_SERVICE_PORT,
            54343);
    this.serviceTimeout =
        this.azkProps
            .getLong(ContainerizedDispatchManagerProperties.KUBERNETES_SERVICE_CREATION_TIMEOUT_MS,
                60000);
    this.initMountPathPrefixForJobtypes =
        this.azkProps
            .getString(ContainerizedDispatchManagerProperties.KUBERNETES_INIT_MOUNT_PATH_FOR_JOBTYPES,
                DEFAULT_INIT_MOUNT_PATH_PREFIX_FOR_JOBTYPES);
    this.appMountPathPrefixForJobtypes =
        this.azkProps
            .getString(ContainerizedDispatchManagerProperties.KUBERNETES_MOUNT_PATH_FOR_JOBTYPES,
                DEFAULT_APP_MOUNT_PATH_PREFIX_FOR_JOBTYPES);
    this.nscdSocketHostPath =
        this.azkProps
            .getString(ContainerizedDispatchManagerProperties.KUBERNETES_POD_NSCD_SOCKET_HOST_PATH,
                DEFAULT_NSCD_SOCKET_HOST_PATH);
    this.nscdSocketVolumeMountPath =
        this.azkProps.getString(
            ContainerizedDispatchManagerProperties.KUBERNETES_POD_NSCD_SOCKET_VOLUME_MOUNT_PATH,
            DEFAULT_NSCD_SOCKET_VOLUME_MOUNT_PATH);

    try {
      // Path to the configuration file for Kubernetes which contains information about
      // Kubernetes API Server and identity for authentication
      final String kubeConfigPath = this.azkProps
          .getString(ContainerizedDispatchManagerProperties.KUBERNETES_KUBE_CONFIG_PATH);
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
   * Construct the flow override parameter (key) for image version.
   * @param imageType
   * @return flow override param
   */
   private String imageTypeOverrideParam(String imageType) {
     return String.join(".", IMAGE, imageType, VERSION);
   }

  /**
   * This method fetches the complete version set information (Map of jobs and their versions)
   * required to run the flow.
   *
   * @param flowParams
   * @param imageTypesUsedInFlow
   * @return VersionSet
   * @throws ExecutorManagerException
   */
  @VisibleForTesting
  VersionSet fetchVersionSet(final int executionId, Map<String, String> flowParams,
       Set<String> imageTypesUsedInFlow) throws ExecutorManagerException {
     VersionSet versionSet = null;

     try {
       if (flowParams != null &&
           flowParams.containsKey(Constants.FlowParameters.FLOW_PARAM_VERSION_SET_ID)) {
         int versionSetId = Integer.parseInt(flowParams
             .get(Constants.FlowParameters.FLOW_PARAM_VERSION_SET_ID));
         try {
            versionSet = this.versionSetLoader.getVersionSetById(versionSetId).get();

            /*
             * Validate that all images part of the flow are included in the retrieved
             * VersionSet. If there are images that were not part of the retrieved version
             * set, then create a new VersionSet with a superset of all images.
             */
           Set<String> imageVersionsNotFound = new TreeSet<>();
           Map<String, String> overlayMap = new HashMap<>();
           for (String imageType : imageTypesUsedInFlow) {
             if (flowParams.containsKey(imageTypeOverrideParam(imageType))) {
               overlayMap.put(imageType, flowParams.get(imageTypeOverrideParam(imageType)));
             } else if (!(imageType.equals(NOOP_TYPE) || versionSet.getVersion(imageType).isPresent())) {
               logger.info("ExecId: {}, imageType: {} not found in versionSet {}",
                   executionId, imageType, versionSetId);
               imageVersionsNotFound.add(imageType);
             }
           }

           if (!(imageVersionsNotFound.isEmpty() && overlayMap.isEmpty())) {
             // Populate a new Version Set
             logger.info("ExecId: {}, Flow had more imageTypes than specified in versionSet {}. "
                 + "Constructing a new one", executionId, versionSetId);
             VersionSetBuilder versionSetBuilder = new VersionSetBuilder(this.versionSetLoader);
             versionSetBuilder.addElements(versionSet.getImageToVersionMap());
             // The following is a safety check. Just in case: getVersionByImageTypes fails below due to an
             // exception, we will have an incomplete/incorrect versionSet. Setting it null ensures, it will
             // be processed from scratch in the following code block
             versionSet = null;
             if (!imageVersionsNotFound.isEmpty()) {
               versionSetBuilder.addElements(
                   this.imageRampupManager.getVersionByImageTypes(imageVersionsNotFound));
             }
             if (!overlayMap.isEmpty()) {
               versionSetBuilder.addElements(overlayMap);
             }
             versionSet = versionSetBuilder.build();
           }
         } catch (Exception e) {
           logger.error("ExecId: {}, Could not find version set id: {} as specified by flow params. "
               + "Will continue by creating a new one.", executionId, versionSetId);
         }
       }

       if (versionSet == null) {
         // Need to build a version set
         imageTypesUsedInFlow.remove(NOOP_TYPE);  // Remove noop type if exists in the input map
         Map<String, String> versionMap = imageRampupManager.getVersionByImageTypes(imageTypesUsedInFlow);
         // Now we will check the flow params for any override versions provided and apply them
         for (String imageType : imageTypesUsedInFlow) {
           final String imageTypeVersionOverrideParam = imageTypeOverrideParam(imageType);
           if (flowParams != null && flowParams.containsKey(imageTypeVersionOverrideParam)) {
             // We will trust that the user-provided version exists for now. May need to add some validation here!
             versionMap.put(imageType, flowParams.get(imageTypeVersionOverrideParam));
           }
         }

         VersionSetBuilder versionSetBuilder = new VersionSetBuilder(this.versionSetLoader);
         versionSet = versionSetBuilder.addElements(versionMap).build();
       }
     } catch (IOException e) {
       logger.error("ExecId: {}, Exception in fetching the VersionSet. Error msg: {}",
           executionId, e.getMessage());
       throw new ExecutorManagerException(e);
     }
     return versionSet;
  }

  /**
   * @param executionId
   * @param versionSet
   * @param jobTypes
   * @return
   * @throws ExecutorManagerException
   */
  @VisibleForTesting
  V1PodSpec createPodSpec(final int executionId, final VersionSet versionSet,
      SortedSet<String> jobTypes)
      throws ExecutorManagerException {
    final String azkabanBaseImageVersion = getAzkabanBaseImageVersion(versionSet);
    final String azkabanConfigVersion = getAzkabanConfigVersion(versionSet);

    final AzKubernetesV1SpecBuilder v1SpecBuilder =
        new AzKubernetesV1SpecBuilder(this.clusterName, Optional.empty())
            .addFlowContainer(this.flowContainerName,
                azkabanBaseImageVersion, ImagePullPolicy.IF_NOT_PRESENT, azkabanConfigVersion)
            .withResources(this.cpuLimit, this.cpuRequest, this.memoryLimit, this.memoryRequest);

    // Add volume for nscd-socket
    addNscdSocketInVolume(v1SpecBuilder);

    Map<String, String> envVariables = new HashMap<>();
    envVariables.put(ContainerizedDispatchManagerProperties.ENV_VERSION_SET_ID,
        String.valueOf(versionSet.getVersionSetId()));
    // Add env variables to spec builder
    addEnvVariablesToSpecBuilder(v1SpecBuilder, envVariables);

    // Create init container yaml file for each jobType
    addInitContainerForAllJobTypes(executionId, jobTypes, v1SpecBuilder, versionSet);

    return v1SpecBuilder.build();
  }

  /**
   * Adding environment variables in pod spec builder.
   *
   * @param v1SpecBuilder
   * @param envVariables
   */
  private void addEnvVariablesToSpecBuilder(AzKubernetesV1SpecBuilder v1SpecBuilder,
      Map<String, String> envVariables) {
    envVariables.forEach((key, value) -> v1SpecBuilder.addEnvVarToFlowContainer(key, value));
  }

  /**
   * This method is used to add volume for nscd socket.
   *
   * @param v1SpecBuilder
   */
  private void addNscdSocketInVolume(AzKubernetesV1SpecBuilder v1SpecBuilder) {
    v1SpecBuilder
        .addHostPathVolume(NSCD_SOCKET_VOLUME_NAME, this.nscdSocketHostPath, HOST_PATH_TYPE,
            this.nscdSocketVolumeMountPath);
  }

  /**
   *
   * @param executionId
   * @param podSpec
   * @return
   */
  @VisibleForTesting
  V1Pod createPodFromSpec(int executionId, V1PodSpec podSpec) {
    final ImmutableMap<String, String> labels = getLabelsForPod();
    final ImmutableMap<String, String> annotations = getAnnotationsForPod();

    final V1Pod pod = new AzKubernetesV1PodBuilder(getPodName(executionId), this.namespace, podSpec)
       .withPodLabels(labels)
       .withPodAnnotations(annotations)
       .build();
    return pod;
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
    logger.info("ExecId: {}, Jobtypes for flow {} are: {}", executionId, flow.getFlowId(), jobTypes);

    final Map<String, String> flowParam =
        flow.getExecutionOptions().getFlowParameters();

    if (flowParam != null && !flowParam.isEmpty()) {
      logger.info("ExecId: {}, Flow Parameters are: {}", executionId, flowParam);
    }
    // Create all image types by adding azkaban base image, azkaban config and all job types for
    // the flow.
    final Set<String> allImageTypes = new TreeSet<>();
    allImageTypes.add(AZKABAN_BASE_IMAGE);
    allImageTypes.add(AZKABAN_CONFIG);
    allImageTypes.addAll(jobTypes);
    final VersionSet versionSet = fetchVersionSet(executionId, flowParam, allImageTypes);
    final V1PodSpec podSpec = createPodSpec(executionId, versionSet, jobTypes);

    final V1Pod pod = createPodFromSpec(executionId, podSpec);
    String podSpecYaml = Yaml.dump(pod).trim();
    logger.debug("ExecId: {}, Pod spec is {}", executionId, podSpecYaml);

    // TODO: Add version set number and json in flow life cycle event so users can use this
    //   information
    try {
      this.coreV1Api.createNamespacedPod(this.namespace, pod, null, null, null);
      logger.info("ExecId: {}, Dispatched pod for execution.", executionId);
    } catch (ApiException e) {
      logger.error("ExecId: {}, Unable to create Pod: {}", executionId, e.getResponseBody());
      throw new ExecutorManagerException(e);
    }
    // Store version set id in execution_flows for execution_id
    this.executorLoader.updateVersionSetId(executionId, versionSet.getVersionSetId());
  }

  /**
   * TODO: Get azkaban base image version from version set.
   *
   * @return
   */
  private String getAzkabanBaseImageVersion(VersionSet versionSet) {
    return versionSet.getVersion(AZKABAN_BASE_IMAGE).get();
  }

  private String getAzkabanConfigVersion(VersionSet versionSet) {
    return versionSet.getVersion(AZKABAN_CONFIG).get();
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
   * TODO: Check if we need to turn everything into lower case?
   *
   * @param executionId
   * @param jobTypes
   * @param v1SpecBuilder
   * @param versionSet
   * @throws ExecutorManagerException
   */
  private void addInitContainerForAllJobTypes(final int executionId,
      final Set<String> jobTypes, final AzKubernetesV1SpecBuilder v1SpecBuilder,
      final VersionSet versionSet)
      throws ExecutorManagerException {
    for (String jobType: jobTypes) {
      // Skip noop and create init container for the remaining job types.
      if(jobType.equals(NOOP_TYPE)) {
        continue;
      }
      try {
        String imageVersion = versionSet.getVersion(jobType).get();
        v1SpecBuilder.addJobType(jobType, imageVersion, ImagePullPolicy.IF_NOT_PRESENT,
            String.join("/", this.initMountPathPrefixForJobtypes, jobType),
            String.join("/", this.appMountPathPrefixForJobtypes,  jobType));
      } catch (Exception e) {
        throw new ExecutorManagerException("Did not find the version string for image type: " +
            jobType + " in versionSet");
      }
    }
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
    try {
      final AzKubernetesV1ServiceBuilder azKubernetesV1ServiceBuilder =
          new AzKubernetesV1ServiceBuilder(
              "v1Service.yaml");
      final V1Service serviceObject = azKubernetesV1ServiceBuilder
          .withExecId(String.valueOf(executionId))
          .withServiceName(getServiceName(executionId))
          .withNamespace(this.namespace)
          .withApiVersion(SERVICE_API_VERSION_2)
          .withKind(MAPPING)
          .withPort(String.valueOf(this.servicePort))
          .withTimeoutMs(String.valueOf(this.serviceTimeout))
          .build();
      this.coreV1Api.createNamespacedService(this.namespace, serviceObject, null, null, null);
      logger.info("ExecId: {}, Service is created.", executionId);
    } catch (final IOException e) {
      logger.error("ExecId: {}, Unable to create service in Kubernetes. Msg: {}", executionId, e.getMessage());
      throw new ExecutorManagerException(e);
    } catch (final ApiException e) {
      logger.error("ExecId: {}, Unable to create service in Kubernetes. Msg: {} ",
          executionId, e.getResponseBody());
      throw new ExecutorManagerException(e);
    }
  }

  /**
   * This method is used to check whether service should be created in Kubernetes for flow container
   * pod or not.
   *
   * @return
   */
  private boolean isServiceRequired() {
    return this.azkProps
        .getBoolean(ContainerizedDispatchManagerProperties.KUBERNETES_SERVICE_REQUIRED, false);
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
      logger.info("ExecId: {}, Action: Pod Deletion, Pod Name: {}", executionId, podName);
    } catch (ApiException e) {
      logger.error("ExecId: {}, Unable to delete Pod in Kubernetes: {}", executionId, e.getResponseBody());
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
      logger.info("ExecId: {}, Action: Service Deletion, Service Name: {}, code: {}, message: {}",
          executionId,
          serviceName,
          deleteResult.getCode(),
          deleteResult.getMessage());
    } catch (ApiException e) {
      logger.error("ExecId: {}, Unable to delete service in Kubernetes: {}", executionId, e.getResponseBody());
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
