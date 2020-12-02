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

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodBuilder;
import io.kubernetes.client.openapi.models.V1PodSpec;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper Kubernetes Pod builder which internally utilizes
 * @see io.kubernetes.client.openapi.models to create kubernetes "Pod" resource.
 */
public class AzKubernetesV1PodBuilder {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AzKubernetesV1PodBuilder.class);

    private static final String API_VERSION = "v1";
    private static final String KIND = "Pod";

    private final V1ObjectMetaBuilder podMetaBuilder = new V1ObjectMetaBuilder();
    private final V1PodSpec podSpec;

    /**
     * @param podName Name of the kubernetes "Pod" resource unique within the namespace
     * @param podNameSpace Name of the kubernetes "Pod" namespace
     */
    public AzKubernetesV1PodBuilder(String podName, String podNameSpace, V1PodSpec podSpec) {
        LOGGER.info("Creating Pod metadata");
        this.podMetaBuilder
                .withName(podName)
                .withNamespace(podNameSpace);
        this.podSpec = podSpec;
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
     * @return Object containing all details required for submitting request for Pod creation
     */
    public V1Pod build() {
        V1ObjectMeta podMeta = this.podMetaBuilder.build();
        V1Pod v1Pod = new V1PodBuilder()
                .withApiVersion(API_VERSION)
                .withKind(KIND)
                .withMetadata(podMeta)
                .withSpec(this.podSpec)
                .build();
        return v1Pod;
    }
}
