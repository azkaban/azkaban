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

import azkaban.utils.TestUtils;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.util.Yaml;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class AzKubernetesV1ServiceBuilderTest {

  @Test
  public void test() throws IOException {
    AzKubernetesV1ServiceBuilder azKubernetesV1ServiceBuilder = new AzKubernetesV1ServiceBuilder(
        "v1Service.yaml");
    V1Service serviceObject = azKubernetesV1ServiceBuilder
        .withExecId("12345")
        .withServiceName("fc-svc-12345")
        .withNamespace("az-team")
        .withApiVersion("ambassador/v2")
        .withKind("Mapping")
        .withPort("54343")
        .withTimeoutMs("60000")
        .build();
    String serviceYaml = Yaml.dump(serviceObject).trim();
    String readServiceYaml = TestUtils.readResource("v1ServiceTest.yaml", this).trim();
    Assert.assertEquals(readServiceYaml, serviceYaml);
  }
}
