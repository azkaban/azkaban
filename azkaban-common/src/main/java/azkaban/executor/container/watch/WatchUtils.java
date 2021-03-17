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
package azkaban.executor.container.watch;

import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.executor.container.KubernetesContainerizedImpl;
import azkaban.executor.container.watch.KubernetesWatch.PodWatchParams;
import azkaban.utils.Props;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatchUtils {
  private static final Logger logger = LoggerFactory.getLogger(WatchUtils.class);

  /**
   * Utility method for creating a Kubernetes (@link ApiClient} from given Azkaban properties.
   *
   * @param azkProps
   * @return api client
   */
  public static ApiClient createApiClient(Props azkProps) {
    String kubeConfigPath;
    KubeConfig kubeConfig;
    ApiClient apiClient;
    try {
      kubeConfigPath = azkProps
          .getString(ContainerizedDispatchManagerProperties.KUBERNETES_KUBE_CONFIG_PATH);
      logger.info("Creating ApiClient with kubeconfig: " + kubeConfigPath);
      kubeConfig = KubeConfig.loadKubeConfig(Files.newBufferedReader(Paths.get(kubeConfigPath),
          Charset.defaultCharset()));
      apiClient = ClientBuilder.kubeconfig(kubeConfig).build();
    } catch (IOException ioe) {
      RuntimeException re = new RuntimeException("IOException while creating Api client", ioe);
      logger.error("Could not create container Api client", re);
      throw re;
    }
    logger.debug("Created ApiClient with kubeconfig: " + kubeConfigPath);
    return apiClient;
  }

  /**
   * Creates {@link PodWatchParams} using Akzaban properties, with the correct Pod {@code
   * labelSelector} provied by the Kubernetes dispatch implementation.
   *
   * @param azkProps
   * @return
   */
  public static PodWatchParams createPodWatchParams(Props azkProps) {
    String namespace =
        azkProps.getString(ContainerizedDispatchManagerProperties.KUBERNETES_NAMESPACE);
    String labelSelector = KubernetesContainerizedImpl.getLabelSelector(azkProps);
    return new PodWatchParams(namespace, labelSelector);
  }
}
