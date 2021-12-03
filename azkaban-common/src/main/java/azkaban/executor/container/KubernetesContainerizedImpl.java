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

import static azkaban.executor.ExecutionControllerUtils.clusterQualifiedExecId;
import static java.util.Objects.requireNonNull;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.Constants.FlowParameters;
import azkaban.container.models.AzKubernetesV1PodBuilder;
import azkaban.container.models.AzKubernetesV1PodTemplate;
import azkaban.container.models.AzKubernetesV1ServiceBuilder;
import azkaban.container.models.AzKubernetesV1SpecBuilder;
import azkaban.container.models.ImagePullPolicy;
import azkaban.container.models.InitContainerType;
import azkaban.container.models.PodTemplateMergeUtils;
import azkaban.event.Event;
import azkaban.event.EventData;
import azkaban.event.EventHandler;
import azkaban.event.EventListener;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.executor.container.watch.KubernetesWatch;
import azkaban.imagemgmt.models.ImageVersion.State;
import azkaban.imagemgmt.rampup.ImageRampupManager;
import azkaban.imagemgmt.version.VersionInfo;
import azkaban.imagemgmt.version.VersionSet;
import azkaban.imagemgmt.version.VersionSetBuilder;
import azkaban.imagemgmt.version.VersionSetLoader;
import azkaban.metrics.ContainerizationMetrics;
import azkaban.project.ProjectLoader;
import azkaban.spi.EventType;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.custom.QuantityFormatException;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1DeleteOptions;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1Status;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Yaml;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is Kubernetes based implementation for containerization. It has implementation for
 * creation/deletion of Pod and service. For any execution, it will identify version set and create
 * a pod for all the valid jobTypes of a flow.
 */
@Singleton
public class KubernetesContainerizedImpl extends EventHandler implements ContainerizedImpl {

  public static final String DEFAULT_FLOW_CONTAINER_NAME_PREFIX = "az-flow-container";
  public static final String DEFAULT_POD_NAME_PREFIX = "fc-dep";
  public static final String DEFAULT_SERVICE_NAME_PREFIX = "fc-svc";
  public static final String DEFAULT_CLUSTER_NAME = "azkaban";
  public static final String DEFAULT_MAX_CPU = "8";
  public static final String DEFAULT_MAX_MEMORY = "64Gi";
  public static final String DEFAULT_CPU_REQUEST = "1";
  public static final String DEFAULT_MEMORY_REQUEST = "2Gi";
  public static final int DEFAULT_CPU_LIMIT_MULTIPLIER = 1;
  public static final int DEFAULT_MEMORY_LIMIT_MULTIPLIER = 2;
  public static final String MAPPING = "Mapping";
  public static final String SERVICE_API_VERSION_2 = "ambassador/v2";
  public static final String DEFAULT_INIT_MOUNT_PATH_PREFIX_FOR_JOBTYPES = "/data/jobtypes";
  public static final String DEFAULT_APP_MOUNT_PATH_PREFIX_FOR_JOBTYPES =
      "/export/apps/azkaban/azkaban-exec-server/current/plugins/jobtypes";
  public static final String DEFAULT_INIT_MOUNT_PATH_PREFIX_FOR_DEPENDENCIES = "/data/dependencies";
  public static final String DEFAULT_APP_MOUNT_PATH_PREFIX_FOR_DEPENDENCIES =
      "/export/apps/azkaban/azkaban-exec-server/current/plugins/dependencies";
  public static final String IMAGE = "image";
  public static final String VERSION = "version";
  public static final String DEFAULT_SECRET_NAME = "azkaban-k8s-secret";
  public static final String DEFAULT_SECRET_VOLUME = DEFAULT_SECRET_NAME;
  public static final String DEFAULT_SECRET_MOUNTPATH = "/var/azkaban/private";
  public static final String SERVICE_SELECTOR_PREFIX = "flow";
  public static final String POD_APPLICATION_TAG = "azkaban-exec-server";
  public static final String CLUSTER_LABEL_NAME = "cluster";
  public static final String APP_LABEL_NAME = "app";
  public static final String EXECUTION_ID_LABEL_NAME = "execution-id";
  public static final String EXECUTION_ID_LABEL_PREFIX = "execid-";
  public static final String DISABLE_CLEANUP_LABEL_NAME = "cleanup-disabled";
  public static final String DEFAULT_AZKABAN_BASE_IMAGE_NAME = "azkaban-base";
  public static final String DEFAULT_AZKABAN_CONFIG_IMAGE_NAME = "azkaban-config";

  private final String namespace;
  private final ApiClient client;
  private final CoreV1Api coreV1Api;
  private final Props azkProps;
  private final ExecutorLoader executorLoader;
  private final String podPrefix;
  private final String servicePrefix;
  private final String clusterName;
  private final String clusterEnv;
  private final String flowContainerName;
  private String cpuLimit;
  private int cpuLimitMultiplier;
  private final String cpuRequest;
  private final String maxAllowedCPU;
  private String memoryLimit;
  private int memoryLimitMultiplier;
  private final String memoryRequest;
  private final String maxAllowedMemory;
  private final int servicePort;
  private final long serviceTimeout;
  private final VersionSetLoader versionSetLoader;
  private final ImageRampupManager imageRampupManager;
  private final KubernetesWatch kubernetesWatch;
  private final String initMountPathPrefixForJobtypes;
  private final String appMountPathPrefixForJobtypes;
  private final Set<String> dependencyTypes;
  private final String initMountPathPrefixForDependencies;
  private final String appMountPathPrefixForDependencies;
  private final boolean saTokenAutoMount;
  private static final Set<String> INCLUDED_JOB_TYPES = new TreeSet<>(
      String.CASE_INSENSITIVE_ORDER);
  private final String secretName;
  private final String secretVolume;
  private final String secretMountpath;
  private final String podTemplatePath;
  private final EventListener eventListener;
  private final ContainerizationMetrics containerizationMetrics;
  private final String azkabanBaseImageName;
  private final String azkabanConfigImageName;
  private final ProjectLoader projectLoader;


  private static final Logger logger = LoggerFactory
      .getLogger(KubernetesContainerizedImpl.class);

  @Inject
  public KubernetesContainerizedImpl(final Props azkProps,
      final ExecutorLoader executorLoader,
      final VersionSetLoader versionSetLoader,
      final ImageRampupManager imageRampupManager,
      final KubernetesWatch kubernetesWatch,
      final EventListener eventListener,
      final ContainerizationMetrics containerizationMetrics,
      final ProjectLoader projectLoader)
      throws ExecutorManagerException {
    this.azkProps = azkProps;
    this.executorLoader = executorLoader;
    this.versionSetLoader = versionSetLoader;
    this.imageRampupManager = imageRampupManager;
    this.kubernetesWatch = kubernetesWatch;
    this.eventListener = eventListener;
    this.containerizationMetrics = containerizationMetrics;
    this.projectLoader = projectLoader;
    this.addListener(this.eventListener);
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
    // This is utilized to set AZ_CLUSTER ENV variable to the POD containers.
    this.clusterEnv = this.azkProps.getString(ConfigurationKeys.AZKABAN_CLUSTER_ENV,
        this.clusterName);
    this.cpuRequest = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_CPU_REQUEST,
            DEFAULT_CPU_REQUEST);
    this.cpuLimitMultiplier = this.azkProps
        .getInt(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_CPU_LIMIT_MULTIPLIER,
            DEFAULT_CPU_LIMIT_MULTIPLIER);
    this.cpuLimit = this.getResourceLimitFromResourceRequest(this.cpuRequest, this.cpuRequest,
        this.cpuLimitMultiplier);
    this.maxAllowedCPU = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_MAX_ALLOWED_CPU
            , DEFAULT_MAX_CPU);
    this.memoryRequest = this.azkProps.getString(ContainerizedDispatchManagerProperties.
            KUBERNETES_FLOW_CONTAINER_MEMORY_REQUEST, DEFAULT_MEMORY_REQUEST);
    this.memoryLimitMultiplier = this.azkProps
        .getInt(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_MEMORY_LIMIT_MULTIPLIER,
            DEFAULT_MEMORY_LIMIT_MULTIPLIER);
    this.memoryLimit = this.getResourceLimitFromResourceRequest(this.memoryRequest, this.memoryRequest,
        memoryLimitMultiplier);
    this.maxAllowedMemory = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_MAX_ALLOWED_MEMORY,
            DEFAULT_MAX_MEMORY);
    this.servicePort =
        this.azkProps.getInt(ContainerizedDispatchManagerProperties.KUBERNETES_SERVICE_PORT,
            54343);
    this.serviceTimeout =
        this.azkProps
            .getLong(ContainerizedDispatchManagerProperties.KUBERNETES_SERVICE_CREATION_TIMEOUT_MS,
                60000);
    this.initMountPathPrefixForJobtypes =
        this.azkProps
            .getString(
                ContainerizedDispatchManagerProperties.KUBERNETES_INIT_MOUNT_PATH_FOR_JOBTYPES,
                DEFAULT_INIT_MOUNT_PATH_PREFIX_FOR_JOBTYPES);
    this.appMountPathPrefixForJobtypes =
        this.azkProps
            .getString(ContainerizedDispatchManagerProperties.KUBERNETES_MOUNT_PATH_FOR_JOBTYPES,
                DEFAULT_APP_MOUNT_PATH_PREFIX_FOR_JOBTYPES);
    this.dependencyTypes =
        new TreeSet<>(this.azkProps
            .getStringList(ContainerizedDispatchManagerProperties.KUBERNETES_DEPENDENCY_TYPES));
    this.initMountPathPrefixForDependencies =
        this.azkProps
            .getString(
                ContainerizedDispatchManagerProperties.KUBERNETES_INIT_MOUNT_PATH_FOR_DEPENDENCIES,
                DEFAULT_INIT_MOUNT_PATH_PREFIX_FOR_DEPENDENCIES);
    this.appMountPathPrefixForDependencies =
        this.azkProps
            .getString(ContainerizedDispatchManagerProperties.KUBERNETES_MOUNT_PATH_FOR_DEPENDENCIES,
                DEFAULT_APP_MOUNT_PATH_PREFIX_FOR_DEPENDENCIES);
    this.secretName = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_SECRET_NAME,
            DEFAULT_SECRET_NAME);
    this.secretVolume = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_SECRET_VOLUME,
            DEFAULT_SECRET_VOLUME);
    this.secretMountpath = this.azkProps
        .getString(
            ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_SECRET_MOUNTPATH,
            DEFAULT_SECRET_MOUNTPATH);
    this.podTemplatePath = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_POD_TEMPLATE_PATH,
            StringUtils.EMPTY);
    this.azkabanBaseImageName = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_POD_AZKABAN_BASE_IMAGE_NAME,
            DEFAULT_AZKABAN_BASE_IMAGE_NAME);
    this.azkabanConfigImageName = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_POD_AZKABAN_CONFIG_IMAGE_NAME,
            DEFAULT_AZKABAN_CONFIG_IMAGE_NAME);
    this.saTokenAutoMount = this.azkProps
        .getBoolean(
            ContainerizedDispatchManagerProperties.KUBERNETES_POD_SERVICE_ACCOUNT_TOKEN_AUTOMOUNT,
            false);
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
    // Add all the job types that are readily available as part of azkaban base image.
    this.addIncludedJobTypes();
  }

  /**
   * Populate the included job types set with all the types that are readily available as part of
   * azkaban base image.
   */
  private void addIncludedJobTypes() {
    INCLUDED_JOB_TYPES.add("hadoopJava");
    INCLUDED_JOB_TYPES.add("hadoopShell");
    INCLUDED_JOB_TYPES.add("hive");
    INCLUDED_JOB_TYPES.add("java");
    INCLUDED_JOB_TYPES.add("java2");
    INCLUDED_JOB_TYPES.add("pig");
    INCLUDED_JOB_TYPES.add("pigLi");
    INCLUDED_JOB_TYPES.add("command");
    INCLUDED_JOB_TYPES.add("javaprocess");
    INCLUDED_JOB_TYPES.add("noop");
  }

  /**
   * Check if job type contains in the included job types. If not check if the job type starts with
   * the any of the job types present in the included job type set. For example, in case of pig job
   * type it can contain version such as pigLi-0.11.1. This is nothing but pointing to the different
   * installation pig job. Hence, it just matches the prefix i.e. pigLi which is the actual job type
   * name.
   *
   * @param jobType
   * @return boolean
   */
  private boolean isPresentInIncludedJobTypes(final String jobType) {
    if (INCLUDED_JOB_TYPES.contains(jobType)) {
      return true;
    } else {
      return isStartWithIncludedJobTypes(jobType);
    }
  }

  /**
   * Check if the job type starts with the aay of the job types present in the included job type
   * set. For example, in case of pig job type it can contain version such as pigLi-0.11.1. This is
   * nothing but pointing to the different installation pig job. Hence, it just matches the prefix
   * i.e. pigLi which is the actual job type name.
   *
   * @param jobType
   * @return boolean
   */
  private boolean isStartWithIncludedJobTypes(final String jobType) {
    for (final String includedJobType : INCLUDED_JOB_TYPES) {
      if (jobType.toLowerCase().startsWith(includedJobType.toLowerCase())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Filter out the included job types from the given job types.
   *
   * @param jobTypes
   * @return Set<String>
   */
  private Set<String> filterIncludedJobTypes(final Set<String> jobTypes) {
    return jobTypes.stream()
        .filter(jobType -> !isPresentInIncludedJobTypes(jobType))
        .collect(Collectors.toSet());
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
   *
   * @param imageType
   * @return flow override param
   */
  private String imageTypeOverrideParam(final String imageType) {
    return String.join(".", IMAGE, imageType, VERSION);
  }

  /**
   * This method fetches the complete version set information (Map of jobs and their versions)
   * required to run the flow.
   *
   * @param flowParams Set of flow properties and flow parameters
   * @param imageTypesUsedInFlow
   * @return VersionSet
   * @throws ExecutorManagerException
   */
  @VisibleForTesting
  VersionSet fetchVersionSet(final int executionId, final Map<String, String> flowParams,
      Set<String> imageTypesUsedInFlow, final ExecutableFlow executableFlow)
      throws ExecutorManagerException {
    VersionSet versionSet = null;

    try {
      if (flowParams != null &&
          flowParams.containsKey(Constants.FlowParameters.FLOW_PARAM_VERSION_SET_ID)) {
        final int versionSetId = Integer.parseInt(flowParams
            .get(Constants.FlowParameters.FLOW_PARAM_VERSION_SET_ID));
        try {
          versionSet = this.versionSetLoader.getVersionSetById(versionSetId).get();
          // Validate if the versionSet contains valid version. If not update the correct
          // version using rampup and active image version information.
          final Map<String, VersionInfo> updatedVersionInfoMap =
              this.imageRampupManager.validateAndGetUpdatedVersionMap(executableFlow, versionSet);
          if (!updatedVersionInfoMap.isEmpty()) {
            // Rebuild version set with correct version
            final VersionSetBuilder versionSetBuilder = new VersionSetBuilder(
                this.versionSetLoader);
            versionSetBuilder.addElements(updatedVersionInfoMap);
            versionSet = versionSetBuilder.build();
          }

          /*
           * Validate that all images part of the flow are included in the retrieved
           * VersionSet. If there are images that were not part of the retrieved version
           * set, then create a new VersionSet with a superset of all images.
           */
          final Set<String> imageVersionsNotFound = new TreeSet<>();
          final Map<String, VersionInfo> overlayMap = new HashMap<>();
          for (final String imageType : imageTypesUsedInFlow) {
            if (flowParams.containsKey(imageTypeOverrideParam(imageType))) {
              // Fetches the user overridden version from the database and this will make sure if
              // the overridden version exists/registered on Azkaban database. Hence, it follows a
              // fail fast mechanism to throw exception if the version does not exist for the
              // given image type.
              final VersionInfo versionInfo = this.imageRampupManager.getVersionInfo(imageType,
                  flowParams.get(imageTypeOverrideParam(imageType)),
                  State.getNewAndActiveStateFilter());
              overlayMap.put(imageType, versionInfo);
              logger.info("User overridden image type {} of version {} is used", imageType,
                  versionInfo.getVersion());
            } else if (!(isPresentInIncludedJobTypes(imageType) || versionSet.getVersion(imageType)
                .isPresent())) {
              logger.info("ExecId: {}, imageType: {} not found in versionSet {}",
                  executionId, imageType, versionSetId);
              imageVersionsNotFound.add(imageType);
            }
          }

          if (!(imageVersionsNotFound.isEmpty() && overlayMap.isEmpty())) {
            // Populate a new Version Set
            logger.info("ExecId: {}, Flow had more imageTypes than specified in versionSet {}. "
                + "Constructing a new one", executionId, versionSetId);
            final VersionSetBuilder versionSetBuilder = new VersionSetBuilder(
                this.versionSetLoader);
            versionSetBuilder.addElements(versionSet.getImageToVersionMap());
            // The following is a safety check. Just in case: getVersionByImageTypes fails below due to an
            // exception, we will have an incomplete/incorrect versionSet. Setting it null ensures, it will
            // be processed from scratch in the following code block
            versionSet = null;
            if (!imageVersionsNotFound.isEmpty()) {
              versionSetBuilder.addElements(
                  this.imageRampupManager
                      .getVersionByImageTypes(executableFlow, imageVersionsNotFound,
                          overlayMap.keySet()));
            }
            if (!overlayMap.isEmpty()) {
              versionSetBuilder.addElements(overlayMap);
            }
            versionSet = versionSetBuilder.build();
          }
        } catch (final Exception e) {
          logger.error("ExecId: {}, Could not find version set id: {} as specified by flow params. "
              + "Will continue by creating a new one.", executionId, versionSetId);
        }
      }

      if (versionSet == null) {
        // Need to build a version set
        // Filter all the job types available in azkaban base image from the input image types set
        imageTypesUsedInFlow = this.filterIncludedJobTypes(imageTypesUsedInFlow);

        // Now we will check the flow params for any override versions provided and apply them
        final Map<String, VersionInfo> overlayMap = new HashMap<>();
        for (final String imageType : imageTypesUsedInFlow) {
          final String imageTypeVersionOverrideParam = imageTypeOverrideParam(imageType);
          VersionInfo versionInfo;
          if (flowParams != null && flowParams.containsKey(imageTypeVersionOverrideParam)) {
            // Fetches the user overridden version from the database and this will make sure if
            // the overridden version exists/registered on Azkaban database. Hence, it follows a
            // fail fast mechanism to throw exception if the version does not exist for the
            // given image type.
            // Allow test version override if allow.test.version flow parameter is set to true
            if (flowParams.containsKey(FlowParameters.FLOW_PARAM_ALLOW_IMAGE_TEST_VERSION) &&
                Boolean.TRUE.equals(Boolean
                    .valueOf(flowParams.get(FlowParameters.FLOW_PARAM_ALLOW_IMAGE_TEST_VERSION)))) {
              versionInfo = this.imageRampupManager.getVersionInfo(imageType,
                  flowParams.get(imageTypeVersionOverrideParam),
                  State.getNewActiveAndTestStateFilter());
              overlayMap.put(imageType, versionInfo);
              logger.info("User overridden image type {} of version {} is used", imageType,
                  versionInfo.getVersion());
            } else {
              versionInfo = this.imageRampupManager.getVersionInfo(imageType,
                  flowParams.get(imageTypeVersionOverrideParam),
                  State.getNewAndActiveStateFilter());
              overlayMap.put(imageType, versionInfo);
              logger.info("User overridden image type {} of version {} is used", imageType,
                  versionInfo.getVersion());
            }
          }
        }

        final Map<String, VersionInfo> versionMap =
            this.imageRampupManager.getVersionByImageTypes(executableFlow, imageTypesUsedInFlow,
                overlayMap.keySet());
        final VersionSetBuilder versionSetBuilder = new VersionSetBuilder(this.versionSetLoader);
        versionSetBuilder.addElements(versionMap);
        versionSet = versionSetBuilder.addElements(overlayMap).build();
      }
    } catch (final IOException e) {
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
   * @param dependencyTypes
   * @return
   * @throws ExecutorManagerException
   */
  @VisibleForTesting
  V1PodSpec createPodSpec(final int executionId, final VersionSet versionSet,
      final SortedSet<String> jobTypes, final Set<String> dependencyTypes,
      final Map<String, String> flowParam)
      throws ExecutorManagerException {
    // Gets azkaban base image full path containing version.
    final String azkabanBaseImageFullPath = getAzkabanBaseImageFullPath(versionSet);
    // TODO: check if we need full path for config as well.
    final String azkabanConfigVersion = getAzkabanConfigVersion(versionSet);
    // Get CPU and memory requested for a flow container
    final String flowContainerCPURequest = getFlowContainerCPURequest(flowParam);
    final String flowContainerMemoryRequest = getFlowContainerMemoryRequest(flowParam);

    final AzKubernetesV1SpecBuilder v1SpecBuilder =
        new AzKubernetesV1SpecBuilder(this.clusterEnv, Optional.empty())
            .addFlowContainer(this.flowContainerName,
                azkabanBaseImageFullPath, ImagePullPolicy.IF_NOT_PRESENT, azkabanConfigVersion)
            .withResources(this.cpuLimit, flowContainerCPURequest, this.memoryLimit,
                flowContainerMemoryRequest);

    final Map<String, String> envVariables = new HashMap<>();
    envVariables.put(ContainerizedDispatchManagerProperties.ENV_VERSION_SET_ID,
        String.valueOf(versionSet.getVersionSetId()));
    envVariables.put(ContainerizedDispatchManagerProperties.ENV_FLOW_EXECUTION_ID,
        String.valueOf(executionId));
    setupJavaRemoteDebug(envVariables, flowParam);
    setupDevPod(envVariables, flowParam);
    setupPodEnvVariables(envVariables, flowParam);
    // Add env variables to spec builder
    addEnvVariablesToSpecBuilder(v1SpecBuilder, envVariables);

    // Create init container yaml file for each jobType and dependency
    addInitContainers(executionId, jobTypes, dependencyTypes, v1SpecBuilder, versionSet);


    // Add volume with secrets mounted
    addSecretVolume(v1SpecBuilder);
    return v1SpecBuilder.build();
  }

  @VisibleForTesting
  V1ObjectMeta createPodMetadata(final int executionId, Map<String, String> flowParam) {
    return new V1ObjectMetaBuilder()
        .withName(getPodName(executionId))
        .withNamespace(this.namespace)
        .addToLabels(getLabelsForPod(executionId, flowParam))
        .addToAnnotations(getAnnotationsForPod())
        .build();
  }

  /**
   * This method is used to get cpu request for a flow container. Precedence is defined below. a)
   * Use CPU request set in flow parameter constrained by max allowed cpu set in config b) Use cpu
   * request set in system properties or default which is set in @cpuRequest.
   *
   * @param flowParam
   * @return CPU request for a flow container
   */
  @VisibleForTesting
  String getFlowContainerCPURequest(final Map<String, String> flowParam) {
    if (flowParam == null || flowParam.isEmpty() || !flowParam
        .containsKey(FlowParameters.FLOW_PARAM_FLOW_CONTAINER_CPU_REQUEST)) {
      return this.cpuRequest;
    }
    String userCPURequest =
        flowParam.get(Constants.FlowParameters.FLOW_PARAM_FLOW_CONTAINER_CPU_REQUEST);
    int resourceCompare = compareResources(this.maxAllowedCPU, userCPURequest);
    if (resourceCompare < 0) { // user requested cpu exceeds max allowed cpu
      userCPURequest = this.maxAllowedCPU;
    } else if (resourceCompare ==0) {// if user requested memory has an parse error, or resource
      // type is not correct, cpu request set in the config should be used
      userCPURequest = this.cpuRequest;
    }
    this.cpuLimit = getResourceLimitFromResourceRequest(userCPURequest, this.cpuRequest,
        DEFAULT_CPU_LIMIT_MULTIPLIER);
    return userCPURequest;
  }

  /**
   * This method is used to get memory request for a flow container. Precedence is defined below. a)
   * Use memory request set in flow parameter constrained by max allowed memory set in config b) Use
   * memory request set in system properties or default which is set in @memoryRequest
   *
   * @param flowParam
   * @return Memory request for a flow container
   */
  @VisibleForTesting
  String getFlowContainerMemoryRequest(final Map<String, String> flowParam) {
    if (flowParam == null || flowParam.isEmpty() || !flowParam
        .containsKey(FlowParameters.FLOW_PARAM_FLOW_CONTAINER_MEMORY_REQUEST)) {
      return this.memoryRequest;
    }
    String userMemoryRequest =
        flowParam.get(Constants.FlowParameters.FLOW_PARAM_FLOW_CONTAINER_MEMORY_REQUEST);
    int resourceCompare = compareResources(this.maxAllowedMemory, userMemoryRequest);
    if (resourceCompare < 0) { // user requested memory exceeds max allowed memory
      userMemoryRequest = this.maxAllowedMemory;
    } else if (resourceCompare == 0) {// if user requested memory has an parse error, or resource
      // type is not correct, memory request set in the config should be used
      userMemoryRequest = this.memoryRequest;
    }
    this.memoryLimit = getResourceLimitFromResourceRequest(userMemoryRequest, this.memoryRequest,
        DEFAULT_MEMORY_LIMIT_MULTIPLIER);
    return userMemoryRequest;
  }

  /**
   * This method compares resource 1 (e.g. max allowed resource) with resource 2 (e.g. user
   * requested resource):
   * 1) if resource 1 >= resource 2, return 1;
   * 2) if resource 1 < resource 2, return -1;
   * 3) if resource 1 cannot be compared with resource 2, e.g. parse error, mis-matching resource
   * types, return 0;
   * The format of a Kubernetes quantity indicates the type of resource, e.g. cpu, memory.
   * @param resource1
   * @param resource2
   * @return
   */
  private int compareResources (final String resource1, final String resource2) {
    try {
      final Quantity quantity1 = new Quantity(resource1), quantity2 = new Quantity(resource2);
      if (quantity1.getFormat() == quantity2.getFormat()) {
        return quantity1.getNumber().compareTo(quantity2.getNumber()) < 0 ? -1 : 1;
      }
    } catch (final QuantityFormatException qe) { // only user requested resource in flow
      // parameter could result in exception
      logger.error("QuantityFormatException while parsing user requested resource: " + resource2);
    }
    // Resources cannot be compared, e.g. resources are different (cpu vs. memory) or
    // exception encountered when parsing resource string
    return 0;
  }

  /**
   * This method returns resource limit as a multiplier of requested resource
   * @param resourceRequest
   * @param defaultResourceRequest
   * @return resource limit based on requested resource
   */
  @VisibleForTesting
  String getResourceLimitFromResourceRequest(final String resourceRequest,
      final String defaultResourceRequest, int multiplier) {
    String resourceLimit = defaultResourceRequest;
    try {
      final String PARTS_RE = "[eEinumkKMGTP]+";
      final String[] parts = resourceRequest.split(PARTS_RE);
      final BigDecimal numericValueRequested = new BigDecimal(parts[0]);
      final String suffix = resourceRequest.substring(parts[0].length());
      final BigDecimal numericValueLimit =
          numericValueRequested.multiply(new BigDecimal(multiplier));
      resourceLimit = numericValueLimit + suffix;
    } catch (final NumberFormatException ne) {
      logger.error("NumberFormatException while paring user requested resource numeric value: "
      + resourceRequest);
    }
    return resourceLimit;
  }

  /**
   * This method is used to setup environment variable to enable remote debug on kubernetes flow
   * container. Based on this environment variable, you can decide to enable or disable remote
   * debug.
   *
   * @param envVariables
   * @param flowParam
   */
  private void setupJavaRemoteDebug(final Map<String, String> envVariables,
      final Map<String, String> flowParam) {
    if (flowParam != null && !flowParam.isEmpty() && flowParam
        .containsKey(Constants.FlowParameters.FLOW_PARAM_JAVA_ENABLE_DEBUG)) {
      envVariables.put(ContainerizedDispatchManagerProperties.ENV_JAVA_ENABLE_DEBUG,
          flowParam.get(Constants.FlowParameters.FLOW_PARAM_JAVA_ENABLE_DEBUG));
    }
  }

  /**
   * This method is used to setup environment variable to enable pod as dev pod which can be helpful
   * for testing. Based on this environment variable, you can decide to start the flow container or
   * not.
   *
   * @param envVariables
   * @param flowParam
   */
  private void setupDevPod(final Map<String, String> envVariables,
      final Map<String, String> flowParam) {
    if (flowParam != null && !flowParam.isEmpty() && flowParam
        .containsKey(FlowParameters.FLOW_PARAM_ENABLE_DEV_POD)) {
      envVariables.put(ContainerizedDispatchManagerProperties.ENV_ENABLE_DEV_POD,
          flowParam.get(FlowParameters.FLOW_PARAM_ENABLE_DEV_POD));
    }
  }

  /**
   * This method is used to setup any environment variable for a pod which can be passed from flow
   * parameter. To provide the generic solution, it is adding all the flow parameters starting with
   * (@FlowParameters.FLOW_PARAM_POD_ENV_VAR)
   *
   * @param envVariables
   * @param flowParam
   */
  void setupPodEnvVariables(final Map<String, String> envVariables,
      final Map<String, String> flowParam) {
    if (flowParam != null && !flowParam.isEmpty()) {
      flowParam.forEach((k, v) -> {
        if (k.startsWith(FlowParameters.FLOW_PARAM_POD_ENV_VAR)) {
          envVariables
              .put(StringUtils.removeStart(k, FlowParameters.FLOW_PARAM_POD_ENV_VAR).toUpperCase(),
                  v);
        }
      });
    }
  }

  /**
   * Adding environment variables in pod spec builder.
   *
   * @param v1SpecBuilder
   * @param envVariables
   */
  private void addEnvVariablesToSpecBuilder(final AzKubernetesV1SpecBuilder v1SpecBuilder,
      final Map<String, String> envVariables) {
    envVariables.forEach((key, value) -> v1SpecBuilder.addEnvVarToFlowContainer(key, value));
  }

  /**
   * Disable auto-mounting of service account tokens. Default value is false.
   *
   * @param podSpec pod specification
   */
  private void setSATokenAutomount(V1PodSpec podSpec) {
    podSpec.automountServiceAccountToken(saTokenAutoMount);
  }

  /**
   * Creates a pod instance.
   * @param podMetadata metadata to use during pod instantiation
   * @param podSpec spec to use during pod instantiation
   * @return a {@link V1Pod} instance
   */
  @VisibleForTesting
  V1Pod createPodFromMetadataAndSpec(final V1ObjectMeta podMetadata, final V1PodSpec podSpec) {
    return new AzKubernetesV1PodBuilder(podMetadata, podSpec).build();
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
    final TreeSet<String> jobTypes = ContainerImplUtils.getJobTypesForFlow(flow);
    logger
        .info("ExecId: {}, Jobtypes for flow {} are: {}", executionId, flow.getFlowId(), jobTypes);
    logger
        .info("ExecId: {}, Dependencies for flow {} are: {}", executionId, flow.getFlowId(),
            this.dependencyTypes);

    Map<String, String> flowParam = null;
    if (flow.getExecutionOptions() != null) {
      flowParam = flow.getExecutionOptions().getFlowParameters();
    }
    if (flowParam != null && !flowParam.isEmpty()) {
      logger.info("ExecId: {}, Flow Parameters are: {}", executionId, flowParam);
    }
    // Create all image types by adding azkaban base image, azkaban config and all job types for
    // the flow.
    final Set<String> allImageTypes = new TreeSet<>();
    allImageTypes.add(this.azkabanBaseImageName);
    allImageTypes.add(this.azkabanConfigImageName);
    allImageTypes.addAll(jobTypes);
    allImageTypes.addAll(this.dependencyTypes);
    final VersionSet versionSet = fetchVersionSet(executionId, flowParam, allImageTypes, flow);
    final V1PodSpec podSpec = createPodSpec(executionId, versionSet, jobTypes, this.dependencyTypes, flowParam);
    setSATokenAutomount(podSpec);
    final V1ObjectMeta podMetadata = createPodMetadata(executionId, flowParam);

    // If a pod-template is provided, merge its component definitions into the podSpec.
    if (StringUtils.isNotEmpty(this.podTemplatePath)) {
      try {
        final AzKubernetesV1PodTemplate podTemplate = AzKubernetesV1PodTemplate
            .getInstance(this.podTemplatePath);
        V1PodSpec podSpecFromTemplate = podTemplate.getPodSpecFromTemplate();
        logPodSpecYaml(executionId, podSpecFromTemplate, flowParam, "ExecId: {}, PodSpec template "
            + "before merge: {}");
        PodTemplateMergeUtils.mergePodSpec(podSpec, podSpecFromTemplate);
        logPodSpecYaml(executionId, podSpecFromTemplate, flowParam, "ExecId: {}, PodSpec after "
            + "template merge: {}");

        final V1ObjectMeta podMetadataFromTemplate = podTemplate.getPodMetadataFromTemplate();
        PodTemplateMergeUtils.mergePodMetadata(podMetadata, podMetadataFromTemplate);
      } catch (final IOException e) {
        logger.info("ExecId: {}, Failed to create k8s pod from template: {}", executionId,
            e.getMessage());
        throw new ExecutorManagerException(e);
      }
    }
    final V1Pod pod = createPodFromMetadataAndSpec(podMetadata, podSpec);
    logPodSpecYaml(executionId, pod, flowParam, "ExecId: {}, Pod: {}");

    try {
      this.coreV1Api.createNamespacedPod(this.namespace, pod, null, null, null);
      logger.info("ExecId: {}, Dispatched pod for execution.", executionId);
    } catch (final ApiException e) {
      logger.error("ExecId: {}, Unable to create Pod: {}", executionId, e.getResponseBody());
      throw new ExecutorManagerException(e);
    }
    // Store version set id in execution_flows for execution_id
    this.executorLoader.updateVersionSetId(executionId, versionSet.getVersionSetId());
    // Marking flow as PREPARING from DISPATCHING as POD creation request is submitted
    flow.setStatus(Status.PREPARING);
    flow.setVersionSet(versionSet);
    this.executorLoader.updateExecutableFlow(flow);
    // Record time taken to dispatch flow to a container
    if (flow.getSubmitTime()>0) {
      final long containerDispatchDuration = System.currentTimeMillis() - flow.getSubmitTime();
      this.containerizationMetrics.addTimeToDispatch(containerDispatchDuration);
    }
    // Emit preparing flow event with version set
    this.fireEventListeners(Event.create(flow, EventType.FLOW_STATUS_CHANGED, new EventData(flow)));
  }

  /**
   * This method is used to log pod spec yaml for debugging purpose. If Pod is marked as dev pod
   * then pod spec yaml will be printed in logs for INFO level else it will be logged for DEBUG
   * level.
   * @param executionId
   * @param podObject Pod/PodSpec depending on the log
   * @param flowParam
   */
  private static void logPodSpecYaml(final int executionId, final Object podObject,
      final Map<String, String> flowParam, String message) {
    final String podSpecYaml = Yaml.dump(podObject).trim();
    if (flowParam != null && !flowParam.isEmpty() && flowParam
        .containsKey(FlowParameters.FLOW_PARAM_ENABLE_DEV_POD)) {
      logger.info(message, executionId, podSpecYaml);
    } else {
      logger.debug(message, executionId, podSpecYaml);
    }
  }

  /**
   * TODO: Get azkaban base image version from version set.
   *
   * @return
   */
  private String getAzkabanBaseImageFullPath(final VersionSet versionSet) {
    return versionSet.getVersion(this.azkabanBaseImageName).get().pathWithVersion();
  }

  private String getAzkabanConfigVersion(final VersionSet versionSet) {
    return versionSet.getVersion(this.azkabanConfigImageName).get().getVersion();
  }

  /**
   * Create labels that should be applied to the Pod.
   *
   * @return
   */
  private ImmutableMap getLabelsForPod(final int executionId, Map<String, String> flowParam) {
    final ImmutableMap.Builder mapBuilder = ImmutableMap.builder();
    mapBuilder.put(CLUSTER_LABEL_NAME, this.clusterName);
    mapBuilder.put(EXECUTION_ID_LABEL_NAME, EXECUTION_ID_LABEL_PREFIX + executionId);
    mapBuilder.put(APP_LABEL_NAME, POD_APPLICATION_TAG);

    // Note that the service label must match the selector used for the corresponding service
    if (isServiceRequired()) {
      mapBuilder.put("service", String.join("-", SERVICE_SELECTOR_PREFIX,
          clusterQualifiedExecId(this.clusterName, executionId)));
    }

    // Set the label for disabling pod-cleanup.
    if (flowParam != null && !flowParam.isEmpty() && flowParam
        .containsKey(FlowParameters.FLOW_PARAM_DISABLE_POD_CLEANUP)) {
      mapBuilder.put(DISABLE_CLEANUP_LABEL_NAME,
          flowParam.get(FlowParameters.FLOW_PARAM_DISABLE_POD_CLEANUP));
    }
    return mapBuilder.build();
  }

  /**
   * Get a {@code lableSelector} that can be used to list all the flow-container-pods for the
   * current namespace.
   *   Example Selector: 'cluster=cluster1,app=azkaban-exec-server'
   *
   * @return label selector
   */
  public static String getLabelSelector(final Props azkProps) {
    requireNonNull(azkProps, "azkaban properties must not be null");
    final String clusterName = azkProps.getString(ConfigurationKeys.AZKABAN_CLUSTER_NAME,
        DEFAULT_CLUSTER_NAME);
    final StringBuilder selectorBuilder = new StringBuilder();
    selectorBuilder.append(CLUSTER_LABEL_NAME + "=" + clusterName).append(",")
        .append(APP_LABEL_NAME + "=" + POD_APPLICATION_TAG);
    return selectorBuilder.toString();
  }

  public String getNamespace() {
    return this.namespace;
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
   * @param dependencyTypes
   * @param v1SpecBuilder
   * @param versionSet
   * @throws ExecutorManagerException
   */
  private void addInitContainers(final int executionId,
      final Set<String> jobTypes, final Set<String> dependencyTypes,
      final AzKubernetesV1SpecBuilder v1SpecBuilder,
      final VersionSet versionSet)
      throws ExecutorManagerException {
    for (final String jobType : jobTypes) {
      // Skip all the job types that are available in the azkaban base image and create init
      // container for the remaining job types.
      if (isPresentInIncludedJobTypes(jobType)) {
        continue;
      }
      try {
        final String imageFullPath = versionSet.getVersion(jobType).get().pathWithVersion();
        v1SpecBuilder.addInitContainerType(jobType, imageFullPath, ImagePullPolicy.IF_NOT_PRESENT,
            String.join("/", this.initMountPathPrefixForJobtypes, jobType),
            String.join("/", this.appMountPathPrefixForJobtypes, jobType), InitContainerType.JOBTYPE);
      } catch (final Exception e) {
        throw new ExecutorManagerException("Did not find the version string for image type: " +
            jobType + " in versionSet");
      }
    }
    for (final String dependency: dependencyTypes) {
      try {
        final String imageFullPath = versionSet.getVersion(dependency).get().pathWithVersion();
        v1SpecBuilder
            .addInitContainerType(dependency, imageFullPath, ImagePullPolicy.IF_NOT_PRESENT,
                String.join("/", this.initMountPathPrefixForDependencies, dependency),
                String.join("/", this.appMountPathPrefixForDependencies, dependency),
                InitContainerType.DEPENDENCY);
      } catch (final Exception e) {
        throw new ExecutorManagerException("Did not find the version string for image type: " +
            dependency + " in versionSet");
      }
    }
  }

  private void addSecretVolume(final AzKubernetesV1SpecBuilder v1SpecBuilder) {
    v1SpecBuilder.addSecretVolume(this.secretVolume, this.secretName, this.secretMountpath);
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
          .withExecId(clusterQualifiedExecId(this.clusterName, executionId))
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
      logger.error("ExecId: {}, Unable to create service in Kubernetes. Msg: {}", executionId,
          e.getMessage());
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
    } catch (final ApiException e) {
      logger.error("ExecId: {}, Unable to delete Pod in Kubernetes: {}", executionId,
          e.getResponseBody());
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
    } catch (final ApiException e) {
      logger.error("ExecId: {}, Unable to delete service in Kubernetes: {}", executionId,
          e.getResponseBody());
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

  public String getCpuLimit() {
    return this.cpuLimit;
  }

  public String getMemoryLimit() {
    return this.memoryLimit;
  }
}
