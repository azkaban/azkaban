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
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    mergeFlowContainer(podSpec, podTemplate);
  }

  private static void mergeFlowContainer(V1PodSpec podSpec, AzKubernetesV1PodTemplate podTemplate) {
    V1Container podSpecFlowContainer = podSpec.getContainers()
        .get(AzKubernetesV1PodTemplate.FLOW_CONTAINER_INDEX);
    V1Container podTemplateFlowContainer = podTemplate.getFlowContainer();
    mergeTemplateAndPodSpecContainer(podTemplateFlowContainer, podSpecFlowContainer);
    podSpec.setContainers(podTemplate.getAllContainers());
  }

  /**
   * Merge InitContainers from the dynamically generated pod-spec and podTemplate, such that: 1) Add
   * all the init containers which are only part of podTemplate. 2) Add all the init containers
   * which are only part of pod-spec. 3) Add the init containers which are part of both pod-spec and
   * pod-template by merging them such that: a) Skeleton of templateInitContainer is utilized. b)
   * Environment variables from podSpecInitContainer are added to corresponding
   * templateInitContainer. c) ImagePullPolicy, Image, and VolumeMounts are overridden from
   * podSpecInitContainer to templateInitContainer.
   *
   * @param podSpec     Already created podSpec using the {@link AzKubernetesV1SpecBuilder}
   * @param podTemplate Instance of the class {@link AzKubernetesV1PodTemplate} to extract items
   */
  private static void mergeInitContainers(V1PodSpec podSpec,
      AzKubernetesV1PodTemplate podTemplate) {
    List<V1Container> podSpecInitContainers = podSpec.getInitContainers();
    if (null == podSpecInitContainers) {
      return;
    }
    // Get init containers from podTemplate which are not part of pod-spec init containers.
    final List<V1Container> templateOnlyInitContainers = podTemplate.getInitContainers(
        templateInitContainer -> podSpecInitContainers.stream().map(V1Container::getName)
            .noneMatch(name -> name.equals(templateInitContainer.getName())));

    // Get init containers from podTemplate which are also part of pod-spec init containers
    // i.e. the other containers apart from the above list templateOnlyInitContainers.
    final List<V1Container> templateAlsoInitContainers = podTemplate.getInitContainers(
        templateInitContainer -> templateOnlyInitContainers.stream().map(V1Container::getName)
            .noneMatch(name -> name.equals(templateInitContainer.getName())));

    // Get init containers from pod-spec which are not part of podTemplate init containers.
    final List<V1Container> podSpecOnlyInitContainers =
        podSpec.getInitContainers().stream().filter(
            podSpecInitContainer -> templateAlsoInitContainers.stream().map(V1Container::getName)
                .noneMatch(name -> name.equals(podSpecInitContainer.getName()))
        ).collect(Collectors.toList());

    // Get init containers from pod-spec which are also part of podTemplate init containers
    // i.e. the other containers apart from the above list podSpecOnlyInitContainers.
    final Map<String, V1Container> podSpecAlsoInitContainers =
        podSpec.getInitContainers().stream().filter(
            podSpecInitContainer -> podSpecOnlyInitContainers.stream().map(V1Container::getName)
                .noneMatch(name -> name.equals(podSpecInitContainer.getName()))
        ).collect(Collectors.toMap(V1Container::getName, e -> e));

    final List<V1Container> allInitContainers = new ArrayList<>();
    allInitContainers.addAll(podSpecOnlyInitContainers);
    allInitContainers.addAll(templateOnlyInitContainers);

    // Merge templateAlsoInitContainers and podSpecAlsoInitContainers.
    List<V1Container> mergedInitContainers = getMergedInitContainers(templateAlsoInitContainers,
        podSpecAlsoInitContainers);
    allInitContainers.addAll(mergedInitContainers);

    // This resets the init containers already part of pod-spec.
    podSpec.setInitContainers(allInitContainers);
  }

  /**
   * This method combines the templateAlsoInitContainers with their corresponding
   * podSpecAlsoInitContainers.
   *
   * @param templateAlsoInitContainers List of all initContainers form the pod-spec template which
   *                                   are also part of dynamically generated pod-spec.
   * @param podSpecAlsoInitContainers  List of all initContainers from the dynamically generated
   *                                   pod-spec which are also part of pod-spec template.
   * @return List of InitContainers by merging templateAlsoInitContainers and podAlsoInitContainers.
   */
  private static List<V1Container> getMergedInitContainers(
      List<V1Container> templateAlsoInitContainers,
      Map<String, V1Container> podSpecAlsoInitContainers) {
    final List<V1Container> mergedInitContainers = new ArrayList<>();
    for (V1Container templateInitContainer : templateAlsoInitContainers) {
      V1Container podSpecInitContainer =
          podSpecAlsoInitContainers.get(templateInitContainer.getName());

      mergeTemplateAndPodSpecContainer(templateInitContainer, podSpecInitContainer);

      // Add the modified templateInitContainer with overrides from corresponding
      // podSpecInitContainer.
      mergedInitContainers.add(templateInitContainer);
    }
    return mergedInitContainers;
  }

  /**
   * This method combines the templateContainer and podSpecContainer such that: 1) Skeleton of
   * templateContainer is utilized. 2) Environment variables from podSpecContainer are added to
   * corresponding templateContainer. 3) ImagePullPolicy, Image, and VolumeMounts are overridden
   * from podSpecContainer to templateContainer.
   *
   * @param templateContainer Container from the pod-spec template
   * @param podSpecContainer  Container from the dynamically generated pod-spec
   */
  private static void mergeTemplateAndPodSpecContainer(V1Container templateContainer,
      V1Container podSpecContainer) {
    // Add env from podSpecContainer to templateContainer.
    final List<V1EnvVar> podSpecInitContainerEnv = podSpecContainer.getEnv();
    if (null != podSpecInitContainerEnv && !podSpecInitContainerEnv.isEmpty()) {
      podSpecInitContainerEnv.forEach(templateContainer::addEnvItem);
    }

    // Override name from the podSpecContainer to templateContainer.
    if (null != podSpecContainer.getName()) {
      templateContainer.setName(podSpecContainer.getName());
    }

    // Override ImagePullPolicy from the corresponding podSpecContainer.
    if (null != podSpecContainer.getImagePullPolicy()) {
      templateContainer.setImagePullPolicy(podSpecContainer.getImagePullPolicy());
    }

    // Override Image from the corresponding podSpecContainer.
    if (null != podSpecContainer.getImage()) {
      templateContainer.setImage(podSpecContainer.getImage());
    }

    // Override Resources from the corresponding podSpecContainer.
    if (null != podSpecContainer.getResources()) {
      templateContainer.setResources(podSpecContainer.getResources());
    }

    // Merge Volume Mounts with podSpecContainerVolumeMounts overriding
    // templateContainerVolumeMounts.
    final List<V1VolumeMount> templateContainerVolumeMounts =
        templateContainer.getVolumeMounts();
    final List<V1VolumeMount> podSpecContainerVolumeMounts =
        podSpecContainer.getVolumeMounts();
    final List<V1VolumeMount> mergedContainerVolumeMounts =
        getMergedContainerVolumeMounts(
            templateContainerVolumeMounts, podSpecContainerVolumeMounts);
    templateContainer.setVolumeMounts(mergedContainerVolumeMounts);
  }

  /**
   * This method merges VolumeMounts such that: 1) All VolumeMounts from the
   * podSpecContainerVolumeMounts are added. 2) Only those VolumeMounts of
   * templateContainerVolumeMounts are added which are not already part of
   * podSpecContainerVolumeMounts.
   *
   * @param templateContainerVolumeMounts List of VolumeMounts from templateContainer.
   * @param podSpecContainerVolumeMounts  List of VolumeMounts from podSpecContainer.
   * @return List of VolumeMounts after merging templateInitContainerVolumeMounts and
   * podSpecInitContainerVolumeMounts.
   */
  private static List<V1VolumeMount> getMergedContainerVolumeMounts(
      final List<V1VolumeMount> templateContainerVolumeMounts,
      final List<V1VolumeMount> podSpecContainerVolumeMounts) {
    final List<V1VolumeMount> allContainerVolumeMounts = new ArrayList<>();
    if (null != templateContainerVolumeMounts && null != podSpecContainerVolumeMounts) {
      allContainerVolumeMounts.addAll(podSpecContainerVolumeMounts);
      templateContainerVolumeMounts
          .stream()
          .filter(templateInitContainerVolumeMount -> podSpecContainerVolumeMounts.stream()
              .map(V1VolumeMount::getName)
              .noneMatch(name -> name.equals(templateInitContainerVolumeMount.getName()))
          ).forEach(vm -> allContainerVolumeMounts.add(vm));
    } else if (null != templateContainerVolumeMounts) {
      allContainerVolumeMounts.addAll(templateContainerVolumeMounts);
    } else if (null != podSpecContainerVolumeMounts) {
      allContainerVolumeMounts.addAll(podSpecContainerVolumeMounts);
    }
    return allContainerVolumeMounts;
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
