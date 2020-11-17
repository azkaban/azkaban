package azkaban.container.models;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerBuilder;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarBuilder;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodBuilder;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1ResourceRequirementsBuilder;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeBuilder;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.openapi.models.V1VolumeMountBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A wrapper Kubernetes Pod builder which internally utilizes
 * @see io.kubernetes.client.openapi.models to create kubernetes "Pod" resource
 *
 * This class provides custom builder methods to have tight coupling of
 * volumes and init containers (associated with job-types) with application-container
 * enabling better type safety and compact implementation
 *
 * The Pod-spec created using this builder creates one application-container
 * and multiple init containers corresponding to each jobType
 */
public class AzKubernetesV1PodBuilder {
    private static final String JOBTYPE_VOLUME_PREFIX = "jobtype-volume-";
    private static final String JOBTYPE_INIT_PREFIX = "jobtype-init-";
    private static final String AZ_CLUSTER_KEY = "AZ_CLUSTER";
    private static final String AZ_CONF_VERSION_KEY = "AZ_CONF_VERSION";
    private static final String JOBTYPE_MOUNT_PATH_KEY = "JOBTYPE_MOUNT_PATH";
    private static final String DEFAULT_RESTART_POLICY = "Never";
    private static final String API_VERSION = "v1";
    private static final String KIND = "Pod";

    private final V1ObjectMetaBuilder podMetaBuilder = new V1ObjectMetaBuilder();
    private final V1ContainerBuilder flowContainerBuilder = new V1ContainerBuilder();

    private final List<V1VolumeMount> appVolumeMounts = new ArrayList<>();
    private final List<V1Volume> appVolumes = new ArrayList<>();
    private final List<V1Container> initContainers = new ArrayList<>();

    private final V1EnvVar azClusterName;
    private final String restartPolicy;

    /**
     * @param podName Name of the kubernetes "Pod" resource unique within the namespace
     * @param podNameSpace Name of the kubernetes "Pod" namespace
     * @param azClusterName Name to uniquely represent Azkaban instance requesting creation of Pod
     * @param restartPolicy Optional argument to specify flow container restart policy, otherwise, its "Never"
     */
    public AzKubernetesV1PodBuilder(String podName, String podNameSpace, String azClusterName, Optional<String> restartPolicy) {
        this.podMetaBuilder
                .withName(podName)
                .withNamespace(podNameSpace);
        this.azClusterName = new V1EnvVarBuilder()
                .withName(AZ_CLUSTER_KEY)
                .withValue(azClusterName)
                .build();
        this.restartPolicy = restartPolicy.orElse(DEFAULT_RESTART_POLICY);
    }

    /**
     * @param labels Key-Val containing identifying information and can be utilized
     *               for the kubernetes selector queries
     */
    public AzKubernetesV1PodBuilder withPodLabels(Map<String, String> labels) {
        this.podMetaBuilder.withLabels(labels);
        return this;
    }

    /**
     * @param annotations Key-Val containing non-identifying information
     */
    public AzKubernetesV1PodBuilder withPodAnnotations(Map<String, String> annotations) {
        this.podMetaBuilder.withAnnotations(annotations);
        return this;
    }

    /**
     * @param name Flow-container/ application-container name
     * @param image Docker image path in the image registry
     * @param imagePullPolicy Docker image pull policy
     * @param confVersion Version for the Azkaban configuration resource
     * @return
     */
    public AzKubernetesV1PodBuilder addFlowContainer(String name, String image, String imagePullPolicy, String confVersion) {
        V1EnvVar azConfVersion = new V1EnvVarBuilder()
                .withName(AZ_CONF_VERSION_KEY)
                .withValue(confVersion)
                .build();
        this.flowContainerBuilder
                .withName(name)
                .withImage(image)
                .withImagePullPolicy(imagePullPolicy)
                .withEnv(this.azClusterName, azConfVersion);
        return this;
    }

    /**
     * @param name JobType name to uniquely identify the init container names
     * @param image Docker image path in the image registry
     * @param imagePullPolicy Docker image pull policy
     * @param initMountPath Path to be utilized by the jobType container image
     * @param appMountPath Path mounted to flow-container/application-container corresponding to jobType
     * @return
     */
    public AzKubernetesV1PodBuilder addJobType(String name, String image, String imagePullPolicy, String initMountPath, String appMountPath) {
        String jobTypeVolumeName = JOBTYPE_VOLUME_PREFIX + name;
        V1Volume jobTypeVolume = new V1VolumeBuilder()
                .withName(jobTypeVolumeName)
                .withNewEmptyDir()
                .endEmptyDir()
                .build();
        V1EnvVar jobTypeMountPath = new V1EnvVarBuilder()
                .withName(JOBTYPE_MOUNT_PATH_KEY)
                .withValue(initMountPath)
                .build();
        V1VolumeMount initMountVolume = new V1VolumeMountBuilder()
                .withName(jobTypeVolumeName)
                .withMountPath(initMountPath)
                .build();
        V1VolumeMount appMountVolume = new V1VolumeMountBuilder()
                .withName(jobTypeVolumeName)
                .withMountPath(appMountPath)
                .build();
        V1Container initContainer = new V1ContainerBuilder()
                .withName(JOBTYPE_INIT_PREFIX + name)
                .addToEnv(this.azClusterName, jobTypeMountPath)
                .withImagePullPolicy(imagePullPolicy)
                .withImage(image)
                .withVolumeMounts(initMountVolume)
                .build();

        this.appVolumes.add(jobTypeVolume);
        this.appVolumeMounts.add(appMountVolume);
        this.initContainers.add(initContainer);
        return this;
    }

    /**
     * @param cpuLimit cpu limit for the flow-container/ application-container
     * @param cpuRequest cpu requested for the flow-container/ application-container
     * @param memLimit memory limit for the flow-container/ application-container
     * @param memRequest memory requested for the flow-container/ application-container
     *
     * Strings itself may contain multiplier. Example: 500m for 500 Millis
     */
    public AzKubernetesV1PodBuilder withResources(String cpuLimit, String cpuRequest, String memLimit, String memRequest) {
        V1ResourceRequirements appResourceRequirements = new V1ResourceRequirementsBuilder()
                .addToLimits("cpu", new Quantity(cpuLimit))
                .addToRequests("cpu", new Quantity(cpuRequest))
                .addToLimits("memory", new Quantity(memLimit))
                .addToRequests("memory", new Quantity(memRequest))
                .build();
        this.flowContainerBuilder.withResources(appResourceRequirements);
        return this;
    }

    /**
     * @return Object containing all details required for submitting request for Pod creation
     */
    public V1Pod build() {
        V1ObjectMeta podMeta = this.podMetaBuilder.build();
        V1Container flowContainer = this.flowContainerBuilder
                .withVolumeMounts(this.appVolumeMounts)
                .build();
        V1Pod v1Pod = new V1PodBuilder()
                .withApiVersion(API_VERSION)
                .withKind(KIND)
                .withMetadata(podMeta)
                .withNewSpec()
                .withVolumes(appVolumes)
                .withInitContainers(initContainers)
                .withContainers(flowContainer)
                .withRestartPolicy(this.restartPolicy)
                .endSpec()
                .build();
        return v1Pod;
    }
}
