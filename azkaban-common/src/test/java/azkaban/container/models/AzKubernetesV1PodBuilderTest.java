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
 *
 */

package azkaban.container.models;

import azkaban.utils.TestUtils;
import com.google.common.collect.ImmutableMap;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.util.Yaml;
import java.io.IOException;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class AzKubernetesV1PodBuilderTest {
    @Test
    public void testPodSpec() throws IOException {
        String podName = "az-example";
        String podNameSpace = "az-team";
        String clusterName = "mycluster";
        ImmutableMap<String, String> labels = ImmutableMap.of("lkey1", "lvalue1");
        ImmutableMap<String, String> annotations = ImmutableMap.of("akey1", "aval1");
        String jobTypeName = "spark";
        String jobTypeImage = "path/spark-jobtype:0.0.5";
        String jobTypeInitMountPath = "/data/jobtypes/spark";
        String jobTypeFlowMountPath = "azBasePath/plugins/jobtypes/spark";
        String flowContainerName = "az-flow-container";
        String flowContainerImage = "path/azkaban-base-image:0.0.5";
        String azConfVer = "0.0.3";
        V1PodSpec podSpec = new AzKubernetesV1SpecBuilder(clusterName, Optional.empty())
            .addJobType(jobTypeName, jobTypeImage, ImagePullPolicy.IF_NOT_PRESENT,
                jobTypeInitMountPath, jobTypeFlowMountPath)
            .addFlowContainer(flowContainerName, flowContainerImage, ImagePullPolicy.IF_NOT_PRESENT,
                azConfVer)
            .addHostPathVolume("nscd-socket", "/var/run/nscd/socket", "Socket",
                "/var/run/nscd/socket")
            .addSecretVolume("azkaban-private-properties", "azkaban-private-properties",
                "/var/azkaban/private/conf")
            .addEnvVarToFlowContainer("envKey", "envValue")
            .withResources("500m", "500m", "500Mi", "500Mi")
            .build();

        V1Pod pod = new AzKubernetesV1PodBuilder(podName, podNameSpace, podSpec)
                .withPodLabels(labels)
                .withPodAnnotations(annotations)
                .build();
        String createdPodSpec = Yaml.dump(pod).trim();
        String readPodSpec = TestUtils.readResource("v1PodTest1.yaml", this).trim();
        Assert.assertEquals(readPodSpec, createdPodSpec);
    }
}
