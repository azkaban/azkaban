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

  public static final String YAML_FILE_NAME = "basicflowyamltest";
  public static final String FLOW_NAME = "basic_flow";
  private Project project;

  @Before
  public void setUp() {
    this.project = new Project(12, "myTestProject");
  }

  @Test
  public void testLoadYamlFileFromDirectory() {
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir(YAML_FILE_NAME));
    Assert.assertEquals(0, loader.getErrors().size());
    Assert.assertEquals(1, this.project.getFlowMap().size());
    Assert.assertTrue(this.project.getFlowMap().containsKey(FLOW_NAME));
    final Flow flow = this.project.getFlowMap().get(FLOW_NAME);
    final AzkabanFlow azkabanFlow = flow.getAzkabanFlow();
    Assert.assertEquals(FLOW_NAME, azkabanFlow.getName());

  }
}