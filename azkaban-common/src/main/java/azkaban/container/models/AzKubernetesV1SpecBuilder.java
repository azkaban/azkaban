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

package azkaban.container.models;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerBuilder;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarBuilder;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodSpecBuilder;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1ResourceRequirementsBuilder;
import io.kubernetes.client.openapi.models.V1SecretVolumeSource;
import io.kubernetes.client.openapi.models.V1SecretVolumeSourceBuilder;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeBuilder;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.openapi.models.V1VolumeMountBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides custom builder methods to have tight coupling of
 * volumes and init containers (associated with job-types) with application-container
 * enabling better type safety and compact implementation.
 *
 * The Pod-spec created using this builder creates one application-container
 * and multiple init containers corresponding to each jobType.
 */
public class AzKubernetesV1SpecBuilder {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AzKubernetesV1SpecBuilder.class);
    private static final String JOBTYPE_VOLUME_PREFIX = "jobtype-volume-";
    private static final String JOBTYPE_INIT_PREFIX = "jobtype-init-";
    private static final String AZ_CLUSTER_KEY = "AZ_CLUSTER";
    private static final String AZ_CONF_VERSION_KEY = "AZ_CONF_VERSION";
    private static final String JOBTYPE_MOUNT_PATH_KEY = "JOBTYPE_MOUNT_PATH";
    private static final String DEFAULT_RESTART_POLICY = "Never";
    private static final int SECRET_VOLUME_DEFAULT_MODE = 0400; // file read permitted only for the user

    private final V1ContainerBuilder flowContainerBuilder = new V1ContainerBuilder();

    private final List<V1VolumeMount> appVolumeMounts = new ArrayList<>();
    private final List<V1Volume> appVolumes = new ArrayList<>();
    private final List<V1Container> initContainers = new ArrayList<>();

    private final V1EnvVar azClusterName;
    private final String restartPolicy;

    /**
     * @param azClusterName Name to uniquely represent Azkaban instance requesting creation of Pod
     * @param restartPolicy Optional argument to specify flow container restart policy, otherwise, its "Never"
     */
    public AzKubernetesV1SpecBuilder(String azClusterName, Optional<String> restartPolicy) {
        this.azClusterName = new V1EnvVarBuilder()
                .withName(AZ_CLUSTER_KEY)
                .withValue(azClusterName)
                .build();
        this.restartPolicy = restartPolicy.orElse(DEFAULT_RESTART_POLICY);
    }

    /**
     * @param envKey Key for the environment variable to be added to the flow container.
     * @param envVal Value for the environment variable to be added to the flow container.
     */
    public AzKubernetesV1SpecBuilder addEnvVarToFlowContainer(final String envKey,
        final String envVal) {
        this.flowContainerBuilder.addNewEnv().withName(envKey).withValue(envVal).endEnv();
        return this;
    }

    /**
     * @param name Flow-container/ application-container name
     * @param image Docker image path in the image registry
     * @param imagePullPolicy Docker image pull policy
     * @param confVersion Version for the Azkaban configuration resource
     *
     * This method adds the configured application-container to the Pod spec.
     * This application container is responsible for executing flow.
     */
    public AzKubernetesV1SpecBuilder addFlowContainer(String name, String image, ImagePullPolicy imagePullPolicy, String confVersion) {
        V1EnvVar azConfVersion = new V1EnvVarBuilder()
                .withName(AZ_CONF_VERSION_KEY)
                .withValue(confVersion)
                .build();
        this.flowContainerBuilder
                .withName(name)
                .withImage(image)
                .withImagePullPolicy(imagePullPolicy.getPolicyVal())
                .withEnv(this.azClusterName, azConfVersion);
        LOGGER.info("Created flow container object with name " + name);
        return this;
    }

    /**
     * @param name JobType name to uniquely identify the init container names
     * @param image Docker image path in the image registry
     * @param imagePullPolicy Docker image pull policy
     * @param initMountPath Path to be utilized by the jobType container image
     * @param appMountPath Path mounted to flow-container/application-container corresponding to jobType
     *
     * This method adds configured init container responsible for copying jobType binaries/configs
     * to a volume also mounted to the application container.
     */
    public AzKubernetesV1SpecBuilder addJobType(String name, String image, ImagePullPolicy imagePullPolicy, String initMountPath, String appMountPath) {
        LOGGER.info("Creating spec objects for jobType " + name);
        String jobTypeVolumeName = JOBTYPE_VOLUME_PREFIX + name;
        V1Volume jobTypeVolume = new V1VolumeBuilder()
                .withName(jobTypeVolumeName)
                .withNewEmptyDir()
                .endEmptyDir()
                .build();
        LOGGER.debug("Created jobTypeVolume object with name " + jobTypeVolumeName);
        V1EnvVar jobTypeMountPath = new V1EnvVarBuilder()
                .withName(JOBTYPE_MOUNT_PATH_KEY)
                .withValue(initMountPath)
                .build();
        V1VolumeMount initMountVolume = new V1VolumeMountBuilder()
                .withName(jobTypeVolumeName)
                .withMountPath(initMountPath)
                .build();
        LOGGER.debug("Created volume mount object to jobType init container " + initMountPath);
        V1VolumeMount appMountVolume = new V1VolumeMountBuilder()
                .withName(jobTypeVolumeName)
                .withMountPath(appMountPath)
                .build();
        LOGGER.debug("Created volume mount object to app container " + appMountPath);
        V1Container initContainer = new V1ContainerBuilder()
                .withName(JOBTYPE_INIT_PREFIX + name)
                .addToEnv(this.azClusterName, jobTypeMountPath)
                .withImagePullPolicy(imagePullPolicy.getPolicyVal())
                .withImage(image)
                .withVolumeMounts(initMountVolume)
                .build();
        LOGGER.debug("Created init container object for jobType " + name);

        this.appVolumes.add(jobTypeVolume);
        LOGGER.debug("Added jobType volume to the pod spec");
        this.appVolumeMounts.add(appMountVolume);
        LOGGER.debug("Added jobType volume mount for the application container");
        this.initContainers.add(initContainer);
        LOGGER.debug("Added jobType init container to the pod spec");
        return this;
    }

    /**
     * This method adds a HostPath volume to the pod-spec and also mounts the volume to the flow
     * container.
     *
     * @param volName      Name of the volume.
     * @param hostPath     Path for the hostPath volume.
     * @param hostPathType     Type for the hostPath volume.
     * @param volMountPath Path to be mounted for the flow container.
     */
    public AzKubernetesV1SpecBuilder addHostPathVolume(final String volName, final String hostPath,
        final String hostPathType, final String volMountPath) {
        final V1Volume hostPathVolume = new V1VolumeBuilder().withName(volName).withNewHostPath()
            .withPath(hostPath).withType(hostPathType).endHostPath().build();
        this.appVolumes.add(hostPathVolume);
        final V1VolumeMount hostPathVolMount = new V1VolumeMountBuilder()
            .withName(volName)
            .withMountPath(volMountPath)
            .build();
        this.appVolumeMounts.add(hostPathVolMount);
        return this;
    }

    /**
     * Adds a volume mount populated from a kubernetes secret.
     *
     * @param volName volume name
     * @param secretName secret name
     * @param volMountPath directory where secret will be mounted
     */
    public AzKubernetesV1SpecBuilder addSecretVolume(final String volName,
        final String secretName, final String volMountPath) {
        final V1SecretVolumeSource secretVolumeSource =
            new V1SecretVolumeSourceBuilder()
                .withNewSecretName(secretName)
                .withDefaultMode(SECRET_VOLUME_DEFAULT_MODE)
                .build();
        final V1Volume secretVolume =
            new V1VolumeBuilder()
                .withName(volName)
                .withSecret(secretVolumeSource).build();
        this.appVolumes.add(secretVolume);
        final V1VolumeMount secretVolumeMount = new V1VolumeMountBuilder()
            .withMountPath(volMountPath)
            .withName(volName)
            .build();
        this.appVolumeMounts.add(secretVolumeMount);
        return this;
    }

    /**
     * @param cpuLimit cpu limit for the flow-container/ application-container
     * @param cpuRequest cpu requested for the flow-container/ application-container
     * @param memLimit memory limit for the flow-container/ application-container
     * @param memRequest memory requested for the flow-container/ application-container
     *
     * All the arguments themselves may contain multiplier. Example: 500m for 500 Millis
     */
    public AzKubernetesV1SpecBuilder withResources(String cpuLimit, String cpuRequest, String memLimit, String memRequest) {
        V1ResourceRequirements appResourceRequirements = new V1ResourceRequirementsBuilder()
                .addToLimits("cpu", new Quantity(cpuLimit))
                .addToRequests("cpu", new Quantity(cpuRequest))
                .addToLimits("memory", new Quantity(memLimit))
                .addToRequests("memory", new Quantity(memRequest))
                .build();
        this.flowContainerBuilder.withResources(appResourceRequirements);
        return this;
    }

    public V1PodSpec build() {
        V1Container flowContainer = this.flowContainerBuilder
                .withVolumeMounts(this.appVolumeMounts)
                .build();
        V1PodSpec v1PodSpec = new V1PodSpecBuilder()
                .withVolumes(appVolumes)
                .withInitContainers(initContainers)
                .withContainers(flowContainer)
                .withRestartPolicy(this.restartPolicy)
                .build();
        return v1PodSpec;
    }
}
