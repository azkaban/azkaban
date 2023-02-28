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
import azkaban.flow.Flow;
import azkaban.flow.FlowResourceRecommendation;
import azkaban.imagemgmt.models.ImageVersion.State;
import azkaban.imagemgmt.rampup.ImageRampupManager;
import azkaban.imagemgmt.version.VersionInfo;
import azkaban.imagemgmt.version.VersionSet;
import azkaban.imagemgmt.version.VersionSetBuilder;
import azkaban.imagemgmt.version.VersionSetLoader;
import azkaban.metrics.ContainerizationMetrics;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
import azkaban.spi.EventType;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.custom.QuantityFormatException;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceList;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Yaml;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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

  public static final String DEFAULT_FLOW_CONTAINER_NAME_PREFIX = "az-platform-image";
  public static final String DEFAULT_POD_NAME_PREFIX = "fc-dep";
  public static final String DEFAULT_SERVICE_NAME_PREFIX = "fc-svc";
  public static final String DEFAULT_VPA_NAME_PREFIX = "fc-vpa";
  public static final String DEFAULT_CLUSTER_NAME = "azkaban";
  public static final String DEFAULT_MIN_CPU = "500m";
  public static final String DEFAULT_MIN_MEMORY = "3Gi";
  public static final String DEFAULT_MAX_CPU = "8";
  public static final String DEFAULT_MAX_MEMORY = "64Gi";
  public static final String DEFAULT_CPU_REQUEST = "1";
  public static final String DEFAULT_MEMORY_REQUEST = "2Gi";
  // Expected: 90% usage for buffering
  public static final double DEFAULT_CPU_RECOMMENDATION_MULTIPLIER = 1.1;
  public static final int DEFAULT_CPU_LIMIT_MULTIPLIER = 1;
  // Expected: 70% usage for buffering
  public static final double DEFAULT_MEMORY_RECOMMENDATION_MULTIPLIER = 1.4;
  public static final int DEFAULT_MEMORY_LIMIT_MULTIPLIER = 1;
  public static final String DEFAULT_DISK_REQUEST = "12Gi";
  public static final String DEFAULT_MAX_DISK = "50Gi";
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
  public static final String FLOW_VPA_LABEL_NAME = "flow-vpa";
  public static final String EXECUTION_ID_LABEL_NAME = "execution-id";
  public static final String EXECUTION_ID_LABEL_PREFIX = "execid-";
  public static final String DISABLE_CLEANUP_LABEL_NAME = "cleanup-disabled";
  public static final String DEFAULT_AZKABAN_BASE_IMAGE_NAME = "azkaban-base";
  public static final String DEFAULT_AZKABAN_CONFIG_IMAGE_NAME = "azkaban-config";
  private static final int DEFAULT_EXECUTION_ID = -1;

  private static final VPARecommendation EMPTY_VPA_RECOMMENDATION = new VPARecommendation(null,
      null);
  private static final String DEFAULT_AZKABAN_SECURITY_INIT_IMAGE_NAME = "azkaban-security-init";

  private final String namespace;
  private final ApiClient client;
  private final CoreV1Api coreV1Api;
  private final Props azkProps;
  private final ExecutorLoader executorLoader;
  private final String podPrefix;
  private final String servicePrefix;
  private final String vpaPrefix;
  private final String clusterName;
  private final String clusterEnv;
  private final String flowContainerName;
  private final String jobTypePrefetchUserMap;
  private int vpaRampUp;
  private boolean vpaEnabled;
  private final double cpuRecommendationMultiplier;
  private int cpuLimitMultiplier;
  private final String defaultCpuRequest;
  private final String minAllowedCPU;
  private final String maxAllowedCPU;
  private final double memoryRecommendationMultiplier;
  private int memoryLimitMultiplier;
  private final String defaultMemoryRequest;
  private final String minAllowedMemory;
  private final String maxAllowedMemory;
  private final String diskRequest;
  private final String maxAllowedDisk;
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
  private final boolean prefetchAllCredentials;
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
  private final ProjectManager projectManager;
  private final VPARecommender vpaRecommender;
  private final VPARecommendation maxVpaRecommendation;

  private static final Logger logger = LoggerFactory
      .getLogger(KubernetesContainerizedImpl.class);
  private final String azkabanSecurityInitImageName;

  @Inject
  public KubernetesContainerizedImpl(final Props azkProps,
      final ExecutorLoader executorLoader,
      final VersionSetLoader versionSetLoader,
      final ImageRampupManager imageRampupManager,
      final KubernetesWatch kubernetesWatch,
      final EventListener eventListener,
      final ContainerizationMetrics containerizationMetrics,
      final ProjectManager projectManager,
      final VPARecommender vpaRecommender,
      final ApiClient client)
      throws ExecutorManagerException {
    this.azkProps = azkProps;
    this.executorLoader = executorLoader;
    this.versionSetLoader = versionSetLoader;
    this.imageRampupManager = imageRampupManager;
    this.kubernetesWatch = kubernetesWatch;
    this.eventListener = eventListener;
    this.containerizationMetrics = containerizationMetrics;
    this.projectManager = projectManager;
    this.vpaRecommender = vpaRecommender;
    this.client = client;
    this.coreV1Api = new CoreV1Api(this.client);
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
    this.vpaPrefix = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_VPA_NAME_PREFIX,
            DEFAULT_VPA_NAME_PREFIX);
    this.clusterName = this.azkProps.getString(ConfigurationKeys.AZKABAN_CLUSTER_NAME,
        DEFAULT_CLUSTER_NAME);
    // This is utilized to set AZ_CLUSTER ENV variable to the POD containers.
    this.clusterEnv = this.azkProps.getString(ConfigurationKeys.AZKABAN_CLUSTER_ENV,
        this.clusterName);
    this.vpaRampUp =
        this.azkProps.getInt(ContainerizedDispatchManagerProperties.KUBERNETES_VPA_RAMPUP,
        0);
    this.vpaEnabled =
        this.azkProps.getBoolean(ContainerizedDispatchManagerProperties.KUBERNETES_VPA_ENABLED,
            true);
    this.defaultCpuRequest = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_CPU_REQUEST,
            DEFAULT_CPU_REQUEST);
    this.cpuLimitMultiplier = this.azkProps
        .getInt(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_CPU_LIMIT_MULTIPLIER,
            DEFAULT_CPU_LIMIT_MULTIPLIER);
    this.cpuRecommendationMultiplier = this.azkProps
        .getDouble(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_CPU_RECOMMENDATION_MULTIPLIER,
            DEFAULT_CPU_RECOMMENDATION_MULTIPLIER);
    this.minAllowedCPU = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_MIN_ALLOWED_CPU
            , DEFAULT_MIN_CPU);
    this.maxAllowedCPU = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_MAX_ALLOWED_CPU
            , DEFAULT_MAX_CPU);
    this.defaultMemoryRequest = this.azkProps.getString(ContainerizedDispatchManagerProperties.
            KUBERNETES_FLOW_CONTAINER_MEMORY_REQUEST, DEFAULT_MEMORY_REQUEST);
    this.memoryRecommendationMultiplier = this.azkProps
        .getDouble(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_MEMORY_RECOMMENDATION_MULTIPLIER,
            DEFAULT_MEMORY_RECOMMENDATION_MULTIPLIER);
    this.memoryLimitMultiplier = this.azkProps
        .getInt(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_MEMORY_LIMIT_MULTIPLIER,
            DEFAULT_MEMORY_LIMIT_MULTIPLIER);
    this.minAllowedMemory = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_MIN_ALLOWED_MEMORY
            , DEFAULT_MIN_MEMORY);
    this.maxAllowedMemory = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_MAX_ALLOWED_MEMORY,
            DEFAULT_MAX_MEMORY);
    this.diskRequest = this.azkProps.getString(
        ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_DISK_REQUEST,
            DEFAULT_DISK_REQUEST);
    this.maxAllowedDisk = this.azkProps.getString(
        ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_MAX_ALLOWED_DISK,
            DEFAULT_MAX_DISK);
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
    this.maxVpaRecommendation = new VPARecommendation(this.maxAllowedCPU,
        this.maxAllowedMemory);
    this.prefetchAllCredentials = this.azkProps
        .getBoolean(ContainerizedDispatchManagerProperties.PREFETCH_PROXY_USER_CERTIFICATES,
            false);
    this.azkabanSecurityInitImageName = this.azkProps
        .getString(ContainerizedDispatchManagerProperties.KUBERNETES_POD_AZKABAN_SECURITY_INIT_IMAGE_NAME,
            DEFAULT_AZKABAN_SECURITY_INIT_IMAGE_NAME);
    this.vpaFlowCriteria = new VPAFlowCriteria(azkProps, logger);
    this.jobTypePrefetchUserMap =
        this.azkProps.getString(ContainerizedDispatchManagerProperties.PREFETCH_JOBTYPE_PROXY_USER_MAP, null);
    // Add all the job types that are readily available as part of azkaban base image.
    this.addIncludedJobTypes();
  }

  /**
   * Populate the included job types set with all the types that are readily available as part of
   * azkaban base image.
   */
  private void addIncludedJobTypes() {
    INCLUDED_JOB_TYPES.add("command");
    INCLUDED_JOB_TYPES.add("gobblin");
    INCLUDED_JOB_TYPES.add("hadoopJava");
    INCLUDED_JOB_TYPES.add("hadoopShell");
    INCLUDED_JOB_TYPES.add("hive");
    INCLUDED_JOB_TYPES.add("java");
    INCLUDED_JOB_TYPES.add("java2");
    INCLUDED_JOB_TYPES.add("pig");
    INCLUDED_JOB_TYPES.add("pigLi");
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
  public synchronized void createContainer(final int executionId) throws ExecutorManagerException {
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
  public synchronized void deleteContainer(final int executionId) throws ExecutorManagerException {
    try { // if pod deletion is not successful, the service deletion can still be handled
      deletePod(executionId);
    } finally {
      if (isServiceRequired()) {
        deleteService(executionId);
      }
    }
  }

  /**
   * This method is used to fetch all pods in the current az cluster and namespace that are
   * created a time duration ago
   * @param containerDuration duration between container start timestamp and current timestamp
   * @return Set of container's execution ids
   */
  @Override
  public Set<Integer> getContainersByDuration(final Duration containerDuration) throws ExecutorManagerException {
    // Get the list of pods from current Azkaban cluster and namespace
    final V1PodList podList= this.getListNamespacedPod();

    // Get all execution ids of the pods whose age is older than a certain time duration
    final OffsetDateTime validStartTimeStamp = OffsetDateTime.now().minus(
        containerDuration.toMillis(), ChronoUnit.MILLIS);
    return getExecutionIdsFromPodList(podList, validStartTimeStamp);
  }

  /**
   * This method is used to fetch a list of all pods in the current az cluster and namespace
   * @return
   * @throws ExecutorManagerException
   */
  private V1PodList getListNamespacedPod() throws ExecutorManagerException {
    try {
      // Select pods from current Azkaban cluster and namespace
      final String label =
          CLUSTER_LABEL_NAME + "=" + this.clusterName + "," + APP_LABEL_NAME + "=" + POD_APPLICATION_TAG;
      return this.coreV1Api.listNamespacedPod(
          this.namespace,
          null,
          null,
          null,
          null,
          label,
          null,
          null,
          null,
          null,
          null
      );
    } catch (final ApiException e) {
      logger.error(String.format("Unable to fetch pods in %s.", this.clusterName),
          e.getResponseBody());
      throw new ExecutorManagerException(e);
    }
  }

  /**
   * Obtain a set of execution ids from stale pod list
   * @param podList a stable pod list fetched from current az cluster and namespace
   * @param validStartTimeStamp earliest creation timestamp for a valid pod
   * @return
   */
  @VisibleForTesting
  Set<Integer> getExecutionIdsFromPodList(final V1PodList podList, final OffsetDateTime validStartTimeStamp) {
    final Set<Integer> staleContainerExecIdSet = new HashSet<>();
    for (final V1Pod pod: podList.getItems()) {
      final V1ObjectMeta podMetadata = pod.getMetadata();
      if (podMetadata.getCreationTimestamp().isBefore(validStartTimeStamp)) {
        final String execIdLabel =
            podMetadata.getLabels().getOrDefault(EXECUTION_ID_LABEL_NAME,
            EXECUTION_ID_LABEL_PREFIX + DEFAULT_EXECUTION_ID);
        try {
          final String execId = execIdLabel.substring(execIdLabel.indexOf(
              EXECUTION_ID_LABEL_PREFIX) + EXECUTION_ID_LABEL_PREFIX.length());
          final int id = Integer.valueOf(execId);
          if (id > 0) {
            staleContainerExecIdSet.add(id);
          }
        } catch (final Exception e) {
          logger.error(String.format("Unable to retrieve execution id from pod %s",
              podMetadata.getName()), e.getMessage());
        }
      }
    }
    return staleContainerExecIdSet;
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
              final Set<State> allowedImageStates = getImageVersionState(flowParams);
              final VersionInfo versionInfo = this.imageRampupManager.getVersionInfo(imageType,
                  flowParams.get(imageTypeOverrideParam(imageType)),
                  allowedImageStates);
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
                  State.getNewActiveTestAndStableStateFilter());
              overlayMap.put(imageType, versionInfo);
              logger.info("User overridden image type {} of version {} is used", imageType,
                  versionInfo.getVersion());
            } else {
              final Set<State> allowedImageStates = getImageVersionState(flowParams);
              versionInfo = this.imageRampupManager.getVersionInfo(imageType,
                  flowParams.get(imageTypeVersionOverrideParam),
                  allowedImageStates);
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
   * @param executableFlow
   * @param flowResourceRecommendation
   * @param flowResourceRecommendationMap
   * @param versionSet
   * @param jobTypes
   * @param dependencyTypes
   * @return
   * @throws ExecutorManagerException
   */
  @VisibleForTesting
  V1PodSpec createPodSpec(final ExecutableFlow executableFlow,
      final FlowResourceRecommendation flowResourceRecommendation,
      final ConcurrentHashMap<String, FlowResourceRecommendation> flowResourceRecommendationMap,
      final VersionSet versionSet,
      final SortedSet<String> jobTypes, final Set<String> dependencyTypes,
      final Map<String, String> flowParam)
      throws ExecutorManagerException {
    // Gets execution id
    final int executionId = executableFlow.getExecutionId();
    // Gets azkaban base image full path containing version.
    final String azkabanBaseImageFullPath = getAzkabanBaseImageFullPath(versionSet);
    // TODO: check if we need full path for config as well.
    final String azkabanConfigVersion = getAzkabanConfigVersion(versionSet);
    // Get CPU and memory requested for a flow container
    final VPARecommendation vpaRecommendation = getFlowContainerRecommendedRequests(executableFlow,
        flowResourceRecommendation, flowResourceRecommendationMap);
    logger.info("VPA Recommendation for {} is: cpuRecommendation {}, memoryRecommendation {}",
        executionId, vpaRecommendation == null ? null : vpaRecommendation.getCpuRecommendation(),
        vpaRecommendation == null ? null : vpaRecommendation.getMemoryRecommendation());
    final String flowContainerCPURequest = getFlowContainerCPURequest(flowParam,
        vpaRecommendation.getCpuRecommendation());
    final String flowContainerCPULimit =
        getResourceLimitFromResourceRequest(flowContainerCPURequest, this.defaultCpuRequest,
            this.cpuLimitMultiplier);
    final String flowContainerMemoryRequest = getFlowContainerMemoryRequest(flowParam,
        vpaRecommendation.getMemoryRecommendation());
    final String flowContainerMemoryLimit = getResourceLimitFromResourceRequest(
        flowContainerMemoryRequest, this.defaultMemoryRequest,
        this.memoryLimitMultiplier);
    final String flowContainerDiskRequest = getFlowContainerDiskRequest(flowParam);
    logger.info("Creating pod for execution-id: " + executionId);
    final AzKubernetesV1SpecBuilder v1SpecBuilder =
        new AzKubernetesV1SpecBuilder(this.clusterEnv, Optional.empty())
            .addFlowContainer(this.flowContainerName,
                azkabanBaseImageFullPath, ImagePullPolicy.IF_NOT_PRESENT, azkabanConfigVersion)
            .withResources(flowContainerCPULimit, flowContainerCPURequest, flowContainerMemoryLimit,
                flowContainerMemoryRequest, flowContainerDiskRequest);

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
    addInitContainers(executableFlow, jobTypes, dependencyTypes, v1SpecBuilder,
        versionSet);


    // Add volume with secrets mounted
    addSecretVolume(v1SpecBuilder);
    return v1SpecBuilder.build();
  }

  @VisibleForTesting
  V1ObjectMeta createPodMetadata(final ExecutableFlow executableFlow,
      final int flowResourceRecommendationId, Map<String, String> flowParam) {
    return new V1ObjectMetaBuilder()
        .withName(getPodName(executableFlow.getExecutionId()))
        .withNamespace(this.namespace)
        .addToLabels(getLabelsForPod(executableFlow, flowResourceRecommendationId, flowParam))
        .addToAnnotations(getAnnotationsForPod())
        .build();
  }

  /**
   * Create FlowResourceRecommendation if not exist; Get recommended requests from VPA; Persist
   * recommendation to DB for caching if changed.
   *
   * @param executableFlow
   * @param flowResourceRecommendation
   * @param flowResourceRecommendationMap it is used for updating flowResourceRecommendation
   * @return Nullable CPU request and nullable memory request for a flow container
   */
  @VisibleForTesting
  VPARecommendation getFlowContainerRecommendedRequests(final ExecutableFlow executableFlow,
      final FlowResourceRecommendation flowResourceRecommendation,
      final ConcurrentHashMap<String, FlowResourceRecommendation> flowResourceRecommendationMap) {
    // VPA is disabled globally: do not create VPA object.
    if (!this.vpaEnabled) {
      return EMPTY_VPA_RECOMMENDATION;
    }

    try {
      // Top-down approach to apply maxAllowedMemory first and then find the optimal memory request
      final VPARecommendation vpaRecommendation =
          this.vpaRecommender.getFlowContainerRecommendedRequests(this.namespace,
              FLOW_VPA_LABEL_NAME, this.getVPAName(flowResourceRecommendation.getId()),
              this.flowContainerName, this.cpuRecommendationMultiplier,
              this.memoryRecommendationMultiplier, this.minAllowedCPU,
              this.minAllowedMemory, this.maxAllowedCPU, this.maxAllowedMemory);

      // Migration/Rollout purpose: set up VPA objects even though the feature is not ramped up.
      if (!IsVPAEnabledForFlow(executableFlow)) {
        return EMPTY_VPA_RECOMMENDATION;
      }

      if (vpaRecommendation == null) {
          return maxVpaRecommendation;
      }

      if (!vpaRecommendation.getCpuRecommendation().equals(flowResourceRecommendation.getCpuRecommendation()) ||
          !vpaRecommendation.getMemoryRecommendation().equals(flowResourceRecommendation.getMemoryRecommendation())) {
        flowResourceRecommendation.setCpuRecommendation(vpaRecommendation.getCpuRecommendation());
        flowResourceRecommendation.setMemoryRecommendation(vpaRecommendation.getMemoryRecommendation());
        // Atomic lock-free update
        flowResourceRecommendationMap.put(flowResourceRecommendation.getFlowId(),
            flowResourceRecommendation);
        // We may choose to update DB periodically instead of every time.
        this.projectManager.updateFlowResourceRecommendation(flowResourceRecommendation);
      }

      executableFlow.setVPAEnabled(true);
      return vpaRecommendation;
    } catch (ExecutorManagerException | ProjectManagerException e) {
      logger.error("Cannot apply resource recommendation from VPA for execId {}", executableFlow
          .getExecutionId(), e);
      // It is fine if cpu recommendation or memory recommendation is null here.
      return new VPARecommendation(flowResourceRecommendation.getCpuRecommendation(),
          flowResourceRecommendation.getMemoryRecommendation());
    }
  }

  /**
   * This method is used to get cpu request for a flow container. Precedence is defined below. a)
   * Use max(CPU request set in flow parameter, CPU request recommendation value from VPA) b) Use
   * cpu request set in system properties or default which is set in @cpuRequest.
   * Above result is constrained by min&max allowed cpu set in config.
   *
   * @param flowParam
   * @param CPURequestFromVPA
   * @return CPU request for a flow container
   */
  @VisibleForTesting
  String getFlowContainerCPURequest(final Map<String, String> flowParam,
      @Nullable final String CPURequestFromVPA) {
    String cpuRequest = getRawFlowContainerCPURequest(flowParam, CPURequestFromVPA);
    if (compareResources(cpuRequest, this.minAllowedCPU) <= 0) {
      return this.minAllowedCPU;
    } else if (compareResources(cpuRequest, this.maxAllowedCPU) >= 0) {
      return this.maxAllowedCPU;
    }
    return cpuRequest;
  }

  private String getRawFlowContainerCPURequest(final Map<String, String> flowParam,
      @Nullable final String CPURequestFromVPA) {
    String cpuRequest = null;
    if (flowParam != null && !flowParam.isEmpty() && flowParam
        .containsKey(FlowParameters.FLOW_PARAM_FLOW_CONTAINER_CPU_REQUEST)) {
      cpuRequest = flowParam.get(Constants.FlowParameters.FLOW_PARAM_FLOW_CONTAINER_CPU_REQUEST);

      if (!isValidResource(cpuRequest)) {
        cpuRequest = null;
      }
    }

    if (CPURequestFromVPA != null && isValidResource(CPURequestFromVPA) && (cpuRequest == null ||
        compareResources(cpuRequest, CPURequestFromVPA) < 0)) {
      cpuRequest = CPURequestFromVPA;
    }

    return cpuRequest != null ? cpuRequest : this.defaultCpuRequest;
  }

  /**
   * This method is used to get memory request for a flow container. Precedence is defined below. a)
   * Use max(memory request set in flow parameter, memory request recommendation value from VPA) b)
   * Use memory request set in system properties or default which is set in @memoryRequest
   * Above result is constrained by min&max allowed cpu set in config.
   *
   * @param flowParam
   * @param memoryRequestFromVPA
   * @return Memory request for a flow container
   */
  @VisibleForTesting
  String getFlowContainerMemoryRequest(final Map<String, String> flowParam,
      @Nullable final String memoryRequestFromVPA) {
    String memoryRequest = getRawFlowContainerMemoryRequest(flowParam, memoryRequestFromVPA);
    if (compareResources(memoryRequest, this.minAllowedMemory) <= 0) {
      return this.minAllowedMemory;
    } else if (compareResources(memoryRequest, this.maxAllowedMemory) >= 0) {
      return this.maxAllowedMemory;
    }
    return memoryRequest;
  }

  private String getRawFlowContainerMemoryRequest(final Map<String, String> flowParam,
      @Nullable final String memoryRequestFromVPA) {
    String memoryRequest = null;
    if (flowParam != null && !flowParam.isEmpty() && flowParam
        .containsKey(FlowParameters.FLOW_PARAM_FLOW_CONTAINER_MEMORY_REQUEST)) {
      memoryRequest =
          flowParam.get(Constants.FlowParameters.FLOW_PARAM_FLOW_CONTAINER_MEMORY_REQUEST);

      if (!isValidResource(memoryRequest)) {
        memoryRequest = null;
      }
    }

    if (memoryRequestFromVPA != null && isValidResource(memoryRequestFromVPA) && (memoryRequest == null ||
        compareResources(memoryRequest, memoryRequestFromVPA) < 0)) {
      memoryRequest = memoryRequestFromVPA;
    }

    return memoryRequest != null ? memoryRequest : this.defaultMemoryRequest;
  }

  /**
   * This method is used to get disk request for flow container. Precedence is defined below.
   * a) Use disk request set in flow parameter constrained by max allowed disk set in config
   * b) Use disk request set in system properties or default is set in @diskRequest
   *
   * @param flowParam
   * @return Disk request for a flow container
   */
  @VisibleForTesting
  String getFlowContainerDiskRequest(final Map<String, String> flowParam) {
    if (flowParam == null || flowParam.isEmpty() || !flowParam
        .containsKey(FlowParameters.FLOW_PARAM_FLOW_CONTAINER_DISK_REQUEST)) {
      return this.diskRequest;
    }

    String userDiskRequest =
        flowParam.get(FlowParameters.FLOW_PARAM_FLOW_CONTAINER_DISK_REQUEST);
    int resourceCompare = compareResources(this.maxAllowedDisk, userDiskRequest);
    if (resourceCompare < 0) {
      // User request disk capacity exceeds max allowed disk
      userDiskRequest = this.maxAllowedDisk;
    } else if (resourceCompare == 0) {
      // If comparison fails for any reason, use the value from the config.
      userDiskRequest = this.diskRequest;
    }
    return userDiskRequest;
  }

  /**
   * This method compares resource 1 (e.g. max allowed resource) with resource 2 (e.g. user
   * requested resource):
   * 1) if resource 1 >= resource 2, return 1;
   * 2) if resource 1 < resource 2, return -1;
   * 3) if resource 1 cannot be compared with resource 2, e.g. parse error, return 0;
   * The format of a Kubernetes quantity indicates the type of resource, e.g. cpu, memory.
   * @param resource1
   * @param resource2
   * @return
   */
  private static int compareResources(final String resource1, final String resource2) {
    try {
      final Quantity quantity1 = new Quantity(resource1), quantity2 = new Quantity(resource2);
      return quantity1.getNumber().compareTo(quantity2.getNumber()) < 0 ? -1 : 1;
    } catch (final Exception e) { // only user requested resource in flow
      // parameter could result in exception
      logger.error("Exception while parsing requested resource: {}, {}",
          resource1, resource2);
    }
    // Resources cannot be compared, e.g. resources are different (cpu vs. memory) or
    // exception encountered when parsing resource string
    return 0;
  }

  private static boolean isValidResource(final String resource) {
    try {
      new Quantity(resource);
    } catch (final Exception e) {
      return false;
    }
    return true;
  }

  /**
   * This method returns resource limit as a multiplier of requested resource
   * @param userResourceRequest
   * @param defaultResourceRequest
   * @return resource limit based on requested resource
   */
  @VisibleForTesting
  String getResourceLimitFromResourceRequest(final String userResourceRequest,
      final String defaultResourceRequest, int multiplier) {
    String resourceLimit = defaultResourceRequest;
    try {
      final String PARTS_RE = "[eEinumkKMGTP]+";
      final String[] parts = userResourceRequest.split(PARTS_RE);
      final BigDecimal numericValueRequested = new BigDecimal(parts[0]);
      final String suffix = userResourceRequest.substring(parts[0].length());
      final BigDecimal numericValueLimit =
          numericValueRequested.multiply(new BigDecimal(multiplier));
      resourceLimit = numericValueLimit + suffix;
    } catch (final NumberFormatException ne) {
      logger.error("NumberFormatException while paring user requested resource numeric value: "
      + userResourceRequest);
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
    if (isDevPod(flowParam)) {
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
  private void createPod(final int executionId)
      throws ExecutorManagerException {
    // Fetch execution flow from execution Id.
    final ExecutableFlow flow = this.executorLoader.fetchExecutableFlow(executionId);
    // Fetch flow resource recommendation for the given execution flow
    final ConcurrentHashMap<String, FlowResourceRecommendation> flowResourceRecommendationMap =
        this.projectManager.getProject(flow.getProjectId()).getFlowResourceRecommendationMap();
    // Migration: for all old flows, flow resource recommendation hasn't been generated yet in the DB
    // Ensures flowResourceRecommendation is atomically cloned. Cloning ensures any modification
    // to flowResourceRecommendation will not affect the value stored in flowResourceRecommendationMap
    final FlowResourceRecommendation flowResourceRecommendation =
        flowResourceRecommendationMap.computeIfAbsent(flow.getFlowId(), flowId ->
        this.projectManager.createFlowResourceRecommendation(flow.getProjectId(),
            flowId)).clone();
    flowResourceRecommendationMap.putIfAbsent(flow.getFlowId(), flowResourceRecommendation);

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

    // Check if flow params has user.to.proxy If it does, skip the next step since this overrides
    // all the project and node properties.
    Set<String> proxyUsersMap = new HashSet<>();
    if (flowParam != null && flowParam.containsKey("user.to.proxy")) {
      proxyUsersMap.add(flowParam.get("user.to.proxy"));
    }
    // If there were no flow parameters get the proxy users for each node by loading the
    //project DAG
    proxyUsersMap.addAll(ContainerImplUtils
        .getProxyUsersForFlow(this.projectManager, flow));

    // Finally, if certain jobtypes need specific user credentials we add them to the prefetch list
    if (this.jobTypePrefetchUserMap != null) {
      final Set<String> jobTypeUsersForFlow =
          ContainerImplUtils.getJobTypeUsersForFlow(this.jobTypePrefetchUserMap, jobTypes);
          proxyUsersMap.addAll(jobTypeUsersForFlow);
      }
    // We add the submit user as if no proxy user is mentioned the submit user is the proxy user.
    proxyUsersMap.add(flow.getSubmitUser());

    // Set the collected list of users as a flow object variable to be accessed while creating
    // pod spec template.
    flow.setProxyUsersFromFlowObj(proxyUsersMap);

    // Create all image types by adding azkaban base image, azkaban config and all job types for
    // the flow.
    final Set<String> allImageTypes = new TreeSet<>();
    allImageTypes.add(this.azkabanBaseImageName);
    allImageTypes.add(this.azkabanConfigImageName);
    allImageTypes.addAll(jobTypes);
    allImageTypes.addAll(this.dependencyTypes);
    allImageTypes.add(this.azkabanSecurityInitImageName);
    final VersionSet versionSet = fetchVersionSet(executionId, flowParam, allImageTypes, flow);
    final V1PodSpec podSpec = createPodSpec(flow, flowResourceRecommendation,
        flowResourceRecommendationMap, versionSet,
        jobTypes, this.dependencyTypes, flowParam);
    setSATokenAutomount(podSpec);
    final V1ObjectMeta podMetadata = createPodMetadata(flow, flowResourceRecommendation.getId(),
        flowParam);

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
      this.coreV1Api.createNamespacedPod(
          this.namespace,
          pod,
          null,
          null,
          null,
          null);
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
    if (isDevPod(flowParam)) {
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
  private ImmutableMap getLabelsForPod(final ExecutableFlow executableFlow,
      final int flowResourceRecommendationId, Map<String, String> flowParam) {
    final int executionId = executableFlow.getExecutionId();
    final ImmutableMap.Builder mapBuilder = ImmutableMap.builder();
    mapBuilder.put(CLUSTER_LABEL_NAME, this.clusterName);
    mapBuilder.put(EXECUTION_ID_LABEL_NAME, EXECUTION_ID_LABEL_PREFIX + executionId);
    mapBuilder.put(APP_LABEL_NAME, POD_APPLICATION_TAG);
    // Each azkaban execution pod will share the same flow vpa label if it comes from the same
    // azkaban flow. This label is used for VPA to map multiple azkaban execution pods with one
    // azkaban flow.
    // In VPA, each VPA object will have its own label selector to choose all pods having the
    // same flow vpa label so that it can provide resource recommendation to one azkaban flow.
    mapBuilder.put(FLOW_VPA_LABEL_NAME, this.getVPAName(flowResourceRecommendationId));

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

  @Override
  public void setVPARampUp(final int rampUp) {
    this.vpaRampUp = rampUp;
  }

  @Override
  public int getVPARampUp() {
    return this.vpaRampUp;
  }

  @Override
  public void setVPAEnabled(final boolean enabled) {
    this.vpaEnabled = enabled;
  }

  @Override
  public boolean getVPAEnabled() {
    return this.vpaEnabled;
  }

  /**
   * Return a boolean value indicates whether vertical pod autoscaler is enabled for a given flow
   * based on ramp up rate and global vpa enabled flag
   *
   * @param executableFlow
   * @return vpa enable status
   */
  private boolean IsVPAEnabledForFlow(ExecutableFlow executableFlow) {
    int flowNameHashValMapping = ContainerImplUtils.getFlowNameHashValMapping(executableFlow);
    return vpaEnabled && flowNameHashValMapping <= this.vpaRampUp;
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
   * @param executableFlow
   * @param jobTypes
   * @param dependencyTypes
   * @param v1SpecBuilder
   * @param versionSet
   * @throws ExecutorManagerException
   */
  private void addInitContainers(final ExecutableFlow executableFlow,
      final Set<String> jobTypes, final Set<String> dependencyTypes,
      final AzKubernetesV1SpecBuilder v1SpecBuilder,
      final VersionSet versionSet)
      throws ExecutorManagerException {
    final ExecutableFlow flow = executableFlow;
    final Set<String> proxyUserList = flow.getProxyUsersFromFlowObj();
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
    if (this.prefetchAllCredentials) {
      try {
        final String imageFullPath =
            versionSet.getVersion(this.azkabanSecurityInitImageName).get().pathWithVersion();
        v1SpecBuilder.addSecurityInitContainer(imageFullPath, ImagePullPolicy.IF_NOT_PRESENT,
            InitContainerType.SECURITY, proxyUserList);
      } catch (final Exception e) {
        throw new ExecutorManagerException("Did not find security image. Failed Proxy User Init "
            + "container");
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
      this.coreV1Api.createNamespacedService(this.namespace, serviceObject, null,
          null, null, null);
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
   * This method is used to delete pod in Kubernetes. It will terminate the pod.
   *
   * @param executionId
   * @throws ExecutorManagerException
   */
  private void deletePod(final int executionId) throws ExecutorManagerException {
    final String podName = getPodName(executionId);
    try {
      final GenericKubernetesApi<V1Pod, V1PodList> podClient =
          new GenericKubernetesApi<>(V1Pod.class, V1PodList.class, "",
              "v1", "pods", this.client);
      final int statusCode =
          podClient.delete(this.namespace, podName).throwsApiException().getHttpStatusCode();
      logger.info("ExecId: {}, Action: Pod Deletion, Pod Name: {}, Status: {}", executionId,
          podName, statusCode);
      if (statusCode == 200) {
        logger.info("Pod deletion successful");
      } else if (statusCode == 202 ) {
        logger.info("Pod deletion request accepted, deletion in background");
      } else {
        logger.error("Pod deletion failed");
        throw new ExecutorManagerException("Pod " + podName + "deletion failed");
      }
    } catch (final ApiException e) {
      logger.warn("Exception occurred when deleting Pod {} in Kubernetes: {}", podName, e.getResponseBody());
      throw new ExecutorManagerException(e);
    }
  }

  /**
   * This method is used to delete service in Kubernetes which is created for Pod.
   *
   * @param executionId
   * @throws ExecutorManagerException
   */
  private void deleteService(final int executionId) throws ExecutorManagerException {
    final String serviceName = getServiceName(executionId);
    try {
      // Using GenericKubernetesApi due to a Known issue in K8s Java client and OpenAPIv2:
      // See more here: https://github.com/kubernetes-client/java/issues/86
      final GenericKubernetesApi<V1Service, V1ServiceList> serviceClient =
          new GenericKubernetesApi<>(V1Service.class, V1ServiceList.class, "",
              "v1", "services", this.client);
      final int statusCode =
          serviceClient.delete(this.namespace, serviceName).throwsApiException().getHttpStatusCode();
      logger.info("ExecId: {}, Action: Service Deletion, Service Name: {}, Status: {}",
          executionId, serviceName, statusCode);
      if (statusCode == 200) {
        logger.info("Service deletion successful");
      } else if (statusCode == 202 ) {
        logger.info("Service deletion request accepted, deletion in background");
      } else {
        logger.error("Service deletion failed");
        throw new ExecutorManagerException("Service " + serviceName + "deletion failed");
      }
    } catch (final ApiException e) {
      logger.error("ExecId: {}, Unable to delete service in Kubernetes: {}", executionId,
          e.getResponseBody());
      throw new ExecutorManagerException(e);
    }
  }

  /**
   * This method is used to get vpa name. It will be created using vpa name prefix, azkaban
   * cluster name and flow resource recommendation id.
   *
   * @param recommendationId
   * @return
   */
  private String getVPAName(final int recommendationId) {
    return String.join("-", this.vpaPrefix, this.clusterName, String.valueOf(recommendationId));
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

  private final Set<State> getImageVersionState(Map<String, String> flowParams) {
    if (isDevPod(flowParams)) {
      return State.getAllStates();
    } else {
      return State.getNewActiveAndStableStateFilter();
    }
  }

  private static final boolean isDevPod(Map<String, String> flowParam) {
    if (flowParam != null && !flowParam.isEmpty() && flowParam
        .containsKey(FlowParameters.FLOW_PARAM_ENABLE_DEV_POD)) {
      return true;
    }
    return false;
  }
}
