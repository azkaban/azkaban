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
    private static final String API_VERSION = "v1";
    private static final String KIND = "Pod";

    private final V1PodSpec podSpec;
    private final V1ObjectMeta podMetadata;

    public AzKubernetesV1PodBuilder(final V1ObjectMeta podMetadata, final V1PodSpec podSpec) {
        this.podSpec = podSpec;
        this.podMetadata = podMetadata;
    }

    /**
     * @return Object containing all details required for submitting request for Pod creation
     */
    public V1Pod build() {
        V1Pod v1Pod = new V1PodBuilder()
                .withApiVersion(API_VERSION)
                .withKind(KIND)
                .withMetadata(this.podMetadata)
                .withSpec(this.podSpec)
                .build();
        return v1Pod;
    }
}
