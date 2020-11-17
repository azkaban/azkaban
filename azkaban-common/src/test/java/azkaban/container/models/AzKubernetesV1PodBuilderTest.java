package azkaban.container.models;

import azkaban.utils.TestUtils;
import com.google.common.collect.ImmutableMap;
import io.kubernetes.client.openapi.models.V1Pod;
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
        String imagePolicy = "IfNotPresent";
        String jobTypeInitMountPath = "/data/jobtypes/spark";
        String jobTypeFlowMountPath = "azBasePath/plugins/jobtypes/spark";
        String flowContainerName = "az-flow-container";
        String flowContainerImage = "path/azkaban-base-image:0.0.5";
        String azConfVer = "0.0.3";
        V1Pod pod = new AzKubernetesV1PodBuilder(podName, podNameSpace, clusterName, Optional.empty())
                .withPodLabels(labels)
                .withPodAnnotations(annotations)
                .addJobType(jobTypeName, jobTypeImage, imagePolicy, jobTypeInitMountPath, jobTypeFlowMountPath)
                .addFlowContainer(flowContainerName, flowContainerImage, imagePolicy, azConfVer)
                .withResources("500m", "500m", "500Mi", "500Mi")
                .build();
        String createdPodSpec = Yaml.dump(pod).trim();
        String readPodSpec = TestUtils.readResource("v1PodTest1.yaml", this).trim();
        Assert.assertEquals(readPodSpec, createdPodSpec);
    }
}