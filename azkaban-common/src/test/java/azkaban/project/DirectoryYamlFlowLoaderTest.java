/*
* Copyright 2017 LinkedIn Corp.
*
* Licensed under the Apache License, Version 2.0 (the “License”); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/

package azkaban.project;

import azkaban.flow.Flow;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DirectoryYamlFlowLoaderTest {

  private static final String BASIC_FLOW_YAML_DIR = "basicflowyamltest";
  private static final String MULTIPLE_FLOW_YAML_DIR = "multipleflowyamltest";
  private static final String FLOW_NAME_1 = "basic_flow";
  private static final String FLOW_NAME_2 = "basic_flow2";
  private Project project;

  @Before
  public void setUp() {
    this.project = new Project(12, "myTestProject");
  }

  @Test
  public void testLoadYamlFileFromDirectory() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(BASIC_FLOW_YAML_DIR));
    Assert.assertEquals(0, loader.getErrors().size());
    Assert.assertEquals(1, loader.getFlowMap().size());
    Assert.assertTrue(loader.getFlowMap().containsKey(FLOW_NAME_1));
    final Flow flow = loader.getFlowMap().get(FLOW_NAME_1);
    final AzkabanFlow azkabanFlow = flow.getAzkabanFlow();
    Assert.assertEquals(FLOW_NAME_1, azkabanFlow.getName());
    Assert.assertEquals(4, azkabanFlow.getNodes().size());
  }

  @Test
  public void testLoadMultipleYamlFilesFromDirectory() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(MULTIPLE_FLOW_YAML_DIR));
    Assert.assertEquals(0, loader.getErrors().size());
    Assert.assertEquals(2, loader.getFlowMap().size());
    Assert.assertTrue(loader.getFlowMap().containsKey(FLOW_NAME_1));
    Assert.assertTrue(loader.getFlowMap().containsKey(FLOW_NAME_2));
    final Flow flow2 = loader.getFlowMap().get(FLOW_NAME_2);
    final AzkabanFlow azkabanFlow2 = flow2.getAzkabanFlow();
    Assert.assertEquals(FLOW_NAME_2, azkabanFlow2.getName());
    Assert.assertEquals(3, azkabanFlow2.getNodes().size());
  }
}
