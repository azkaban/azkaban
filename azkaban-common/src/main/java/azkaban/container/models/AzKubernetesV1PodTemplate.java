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

import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.util.Yaml;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * A singleton class which reads a k8s pod-spec yaml file as template.
 * <p>
 * Items are later merged via {@link PodTemplateMergeUtils#mergePodSpec(V1PodSpec, V1PodSpec)}
 * (V1PodSpec, AzKubernetesV1PodTemplate)} with the pod-spec created using {@link
 * AzKubernetesV1SpecBuilder}
 * <p>
 * The template must have only one non-init container. Azkaban k8s design is such that
 * flow-container will be the only non-init container.
 */
public class AzKubernetesV1PodTemplate {

  private static AzKubernetesV1PodTemplate instance;
  private final File templateFile;
  private final String templatePodString;

  /**
   * Private constructor to make this class singleton
   *
   * @param templatePath th where the template file is located.
   */
  private AzKubernetesV1PodTemplate(String templatePath) throws IOException {
    this.templateFile = Paths.get(templatePath).toFile();

    // Rather than reading the template file string, load and dump again. This ensures that if
    // there is a problem with template string, error will be thrown earlier.
    V1Pod v1Pod = (V1Pod) Yaml.load(templateFile);
    this.templatePodString = Yaml.dump(v1Pod);
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
   * Always returns a new instance of V1PodSpec generated from the template.
   * @return the {@link V1PodSpec} POD spec generated from the template.
   * @throws IOException If unable to read the template file.
   */
  public synchronized V1PodSpec getPodSpecFromTemplate() throws IOException {
    return ((V1Pod) Yaml.load(this.templatePodString)).getSpec();
  }
}
