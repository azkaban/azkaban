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
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Probe;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import java.util.List;

/**
 * This class consists of various methods to merge pod-spec with the pod template
 */
public class PodTemplateMergeUtils {

  private PodTemplateMergeUtils() {
    // Not to be instantiated
  }

  /**
   * Items extracted from the podTemplate will be merged with the pod-spec. Merging criteria is such
   * that, if the item, in the already created pod-spec, has the same name as of the item extracted
   * from the template, then it will retained.
   *
   * @param podSpec     Already created podSpec using the {@link AzKubernetesV1SpecBuilder}
   * @param podTemplate Instance of the class {@link AzKubernetesV1PodTemplate} to extract items
   */
  public static void mergePodSpec(V1PodSpec podSpec, AzKubernetesV1PodTemplate podTemplate) {
    mergeVolumes(podSpec, podTemplate);
    mergeInitContainers(podSpec, podTemplate);
    mergeContainerVolumes(podSpec, podTemplate);
    mergeReadinessProbe(podSpec, podTemplate);
    mergeLivelinessProbe(podSpec, podTemplate);
    mergeStartupProbe(podSpec, podTemplate);
  }

  /**
   * Set the StartupProbe for flowContainer derived from the podTemplate to the podSpec
   *
   * @param podSpec     Already created podSpec using the {@link AzKubernetesV1SpecBuilder}
   * @param podTemplate Instance of the class {@link AzKubernetesV1PodTemplate} to extract items
   */
  private static void mergeStartupProbe(V1PodSpec podSpec, AzKubernetesV1PodTemplate podTemplate) {
    V1Probe containerStartupProbe = podTemplate.getContainerStartupProbe();
    if (null != containerStartupProbe) {
      podSpec.getContainers().get(AzKubernetesV1PodTemplate.FLOW_CONTAINER_INDEX)
          .setStartupProbe(containerStartupProbe);
    }
  }

  /**
   * Set the LivelinessProbe for flowContainer derived from the podTemplate to the podSpec
   *
   * @param podSpec     Already created podSpec using the {@link AzKubernetesV1SpecBuilder}
   * @param podTemplate Instance of the class {@link AzKubernetesV1PodTemplate} to extract items
   */
  private static void mergeLivelinessProbe(V1PodSpec podSpec,
      AzKubernetesV1PodTemplate podTemplate) {
    V1Probe containerLivelinessProbe = podTemplate.getContainerLivelinessProbe();
    if (null != containerLivelinessProbe) {
      podSpec.getContainers().get(AzKubernetesV1PodTemplate.FLOW_CONTAINER_INDEX)
          .setLivenessProbe(containerLivelinessProbe);
    }
  }

  /**
   * Set the ReadinessProbe for flowContainer derived from the podTemplate to the podSpec
   *
   * @param podSpec     Already created podSpec using the {@link AzKubernetesV1SpecBuilder}
   * @param podTemplate Instance of the class {@link AzKubernetesV1PodTemplate} to extract items
   */
  private static void mergeReadinessProbe(V1PodSpec podSpec,
      AzKubernetesV1PodTemplate podTemplate) {
    V1Probe containerReadinessProbe = podTemplate.getContainerReadinessProbe();
    if (null != containerReadinessProbe) {
      podSpec.getContainers().get(AzKubernetesV1PodTemplate.FLOW_CONTAINER_INDEX)
          .setReadinessProbe(containerReadinessProbe);
    }
  }

  /**
   * Add those container volumes which are not already available in the podSpec
   *
   * @param podSpec     Already created podSpec using the {@link AzKubernetesV1SpecBuilder}
   * @param podTemplate Instance of the class {@link AzKubernetesV1PodTemplate} to extract items
   */
  private static void mergeContainerVolumes(V1PodSpec podSpec,
      AzKubernetesV1PodTemplate podTemplate) {
    List<V1VolumeMount> podContainerVolumeMounts =
        podSpec.getContainers().get(AzKubernetesV1PodTemplate.FLOW_CONTAINER_INDEX)
            .getVolumeMounts();
    if (null != podContainerVolumeMounts) {
      List<V1VolumeMount> templateContainerVolumeMounts = podTemplate.getContainerVolumeMounts(
          tempVolMount -> podContainerVolumeMounts.stream().map(V1VolumeMount::getName)
              .noneMatch(name -> name.equals(tempVolMount.getName())));
      for (V1VolumeMount volumeMountItem : templateContainerVolumeMounts) {
        podSpec.getContainers().get(AzKubernetesV1PodTemplate.FLOW_CONTAINER_INDEX)
            .addVolumeMountsItem(volumeMountItem);
      }
    }
  }

  /**
   * Add those initContainers which are not already available in the podSpec
   *
   * @param podSpec     Already created podSpec using the {@link AzKubernetesV1SpecBuilder}
   * @param podTemplate Instance of the class {@link AzKubernetesV1PodTemplate} to extract items
   */
  private static void mergeInitContainers(V1PodSpec podSpec,
      AzKubernetesV1PodTemplate podTemplate) {
    List<V1Container> podSpecInitContainers = podSpec.getInitContainers();
    if (null != podSpecInitContainers) {
      List<V1Container> templateInitContainers = podTemplate.getInitContainers(
          tempInitContainer -> podSpecInitContainers.stream().map(V1Container::getName)
              .noneMatch(name -> name.equals(tempInitContainer.getName())));
      for (V1Container containerItem : templateInitContainers) {
        podSpec.addInitContainersItem(containerItem);
      }
    }
  }

  /**
   * Add those volumes which are not already available in the podSpec
   *
   * @param podSpec     Already created podSpec using the {@link AzKubernetesV1SpecBuilder}
   * @param podTemplate Instance of the class {@link AzKubernetesV1PodTemplate} to extract items
   */
  private static void mergeVolumes(V1PodSpec podSpec, AzKubernetesV1PodTemplate podTemplate) {
    List<V1Volume> podSpecVolumes = podSpec.getVolumes();
    if (null != podSpecVolumes) {
      List<V1Volume> templateVolumes = podTemplate.getVolumes(
          tempVol -> podSpecVolumes.stream().map(V1Volume::getName)
              .noneMatch(name -> name.equals(tempVol.getName())));
      for (V1Volume volumeItem : templateVolumes) {
        podSpec.addVolumesItem(volumeItem);
      }
    }
  }

}
