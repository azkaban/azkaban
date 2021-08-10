/*
 * Copyright 2021 LinkedIn Corp.
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

import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Probe;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.util.Yaml;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A singleton class which reads a k8s pod-spec yaml file as template. Items like InitContainers,
 * Volumes and VolumeMounts for the application-container/flow-container will be extracted from the
 * POD created from this template file.
 * <p>
 * Extracted Items are later merged via {@link PodTemplateMergeUtils#mergePodSpec(V1PodSpec, AzKubernetesV1PodTemplate)} (V1PodSpec,
 * AzKubernetesV1PodTemplate)} with the pod-spec created using {@link AzKubernetesV1SpecBuilder}
 * <p>
 * Merging criteria is such that, if the item in the already created pod-spec has the same name as
 * of the item extracted from the template, then it will be retained.
 * <p>
 * This template design enables declaration of static initContainers, volumes, volumeMounts, etc.
 * which are not job-types but are useful. For example: init container which will fetch the required
 * certificates/tokens etc. writes to a volume and then that volume will be mounted to the
 * application-container/flow-container.
 *
 * The template must have only one non-init container. Azkaban k8s design is such that
 * flow-container will be the only non-init container.
 */
public class AzKubernetesV1PodTemplate {

  public static final int FLOW_CONTAINER_INDEX = 0;
  private V1Pod podFromTemplate;
  private static AzKubernetesV1PodTemplate instance;

  /**
   * Private constructor to make this class singleton
   *
   * @param templatePath th where the template file is located.
   * @throws IOException If unable to read the template file.
   */
  private AzKubernetesV1PodTemplate(String templatePath) throws IOException {
    File templateFile = Paths.get(templatePath).toFile();
    this.podFromTemplate = (V1Pod) Yaml.load(templateFile);
  }

  /**
   * @param templatePath Path where the template file is located.
   * @return Singleton instance of this class.
   * @throws IOException If unable to read the template file.
   */
  public static synchronized AzKubernetesV1PodTemplate getInstance(String templatePath)
      throws IOException {
    if (null == instance) {
      instance = new AzKubernetesV1PodTemplate(templatePath);
    }
    return instance;
  }

  /**
   * @return The Flow Container which must be the first container among all app-containers
   */
  public V1Container getFlowContainer() {
    List<V1Container> containers = this.podFromTemplate.getSpec().getContainers();
    return containers.isEmpty() ? null : containers.get(FLOW_CONTAINER_INDEX);
  }

  /**
   * @return The list of all app-containers.
   */
  public List<V1Container> getAllContainers() {
    return this.podFromTemplate.getSpec().getContainers();
  }


  /**
   * @return the {@link V1Pod} POD generated from the template.
   */
  public V1Pod getPodFromTemplate() {
    return this.podFromTemplate;
  }

  /**
   * @param filterPredicate Predicate to filter the init containers.
   * @return The list of filtered init Containers derived from the POD specified in the template.
   */
  public List<V1Container> getInitContainers(Predicate<? super V1Container> filterPredicate) {
    V1PodSpec spec = this.podFromTemplate.getSpec();
    if (null == spec) {
      return Collections.emptyList();
    }
    List<V1Container> initContainers = spec.getInitContainers();
    return initContainers == null ? Collections.emptyList() :
        initContainers.stream().filter(filterPredicate).collect(Collectors.toList());
  }

  /**
   * @return The list of init Containers derived from the POD specified in the template.
   */
  public List<V1Container> getInitContainers() {
    V1PodSpec spec = this.podFromTemplate.getSpec();
    return spec == null ? Collections.emptyList() : spec.getInitContainers();
  }

  /**
   * @param filterPredicate Predicate to filter the volumes.
   * @return The list of filtered Volumes derived from the POD specified in the template.
   */
  public List<V1Volume> getVolumes(Predicate<? super V1Volume> filterPredicate) {
    V1PodSpec spec = this.podFromTemplate.getSpec();
    if (null == spec) {
      return Collections.emptyList();
    }
    List<V1Volume> volumes = spec.getVolumes();
    return volumes == null ?
        Collections.emptyList() :
        volumes.stream().filter(filterPredicate).collect(Collectors.toList());
  }

  /**
   * @return The list of filtered Volumes derived from the POD specified in the template.
   */
  public List<V1Volume> getVolumes() {
    V1PodSpec spec = this.podFromTemplate.getSpec();
    return spec == null ? Collections.emptyList() : spec.getVolumes();
  }

  /**
   * This method returns volume mounts only for the first container.
   *
   * @param filterPredicate Predicate to filter the Volume Mounts.
   * @return The list of filtered Volume Mounts to the appContainer derived from the POD specified
   * in the template.
   */
  public List<V1VolumeMount> getContainerVolumeMounts(
      Predicate<? super V1VolumeMount> filterPredicate) {
    V1PodSpec spec = this.podFromTemplate.getSpec();
    if (null == spec) {
      return Collections.emptyList();
    }
    List<V1Container> containers = spec.getContainers();
    if (null == containers || containers.isEmpty()) {
      return Collections.emptyList();
    }
    List<V1VolumeMount> volumeMounts =
        spec.getContainers().get(FLOW_CONTAINER_INDEX).getVolumeMounts();
    return null == volumeMounts ? Collections.emptyList()
        : volumeMounts.stream().filter(filterPredicate).collect(Collectors.toList());
  }

  /**
   * This method returns volume mounts only for the first container.
   *
   * @return The list of Volume Mounts to the appContainer derived from the POD specified in the
   * template.
   */
  public List<V1VolumeMount> getContainerVolumeMounts() {
    V1PodSpec spec = this.podFromTemplate.getSpec();
    List<V1Container> containers = spec == null ? Collections.emptyList() : spec.getContainers();
    return containers.isEmpty() ? Collections.emptyList() :
        containers.get(FLOW_CONTAINER_INDEX).getVolumeMounts();
  }

  /**
   * @return The Liveliness probe for the appContainer derived form the POD specified in the
   * template.
   */
  public V1Probe getContainerLivelinessProbe() {
    V1PodSpec spec = this.podFromTemplate.getSpec();
    List<V1Container> containers = spec == null ? Collections.emptyList() : spec.getContainers();
    return containers.isEmpty() ? null :
        containers.get(FLOW_CONTAINER_INDEX).getLivenessProbe();
  }

  /**
   * @return The Readiness probe for the appContainer derived form the POD specified in the
   * template.
   */
  public V1Probe getContainerReadinessProbe() {
    V1PodSpec spec = this.podFromTemplate.getSpec();
    List<V1Container> containers = spec == null ? Collections.emptyList() : spec.getContainers();
    return containers.isEmpty() ? null :
        containers.get(FLOW_CONTAINER_INDEX).getReadinessProbe();
  }

  /**
   * @return The Startup probe for the appContainer derived form the POD specified in the template.
   */
  public V1Probe getContainerStartupProbe() {
    V1PodSpec spec = this.podFromTemplate.getSpec();
    List<V1Container> containers = spec == null ? Collections.emptyList() : spec.getContainers();
    return containers.isEmpty() ? null :
        containers.get(FLOW_CONTAINER_INDEX).getStartupProbe();
  }
}
