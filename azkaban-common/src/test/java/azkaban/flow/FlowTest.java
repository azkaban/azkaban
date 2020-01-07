/*
 * Copyright 2017 LinkedIn Corp.
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
package azkaban.flow;

import azkaban.project.DirectoryFlowLoader;
import azkaban.project.Project;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import org.junit.Assert;
import org.junit.Test;


public class FlowTest {

  private Project getEmbeddedTestProject() throws Exception {
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props());

    final Project project = new Project(11, "myTestProject");
    loader.loadProjectFlow(project, ExecutionsTestUtil.getFlowDir("embedded"));
    assert loader.getErrors().size() == 0;

    project.setFlows(loader.getFlowMap());
    project.setVersion(123);
    return project;
  }

  @Test
  public void testNodeLevelComputation() throws Exception {
    final Flow flow = FlowUtils.getFlow(this.getEmbeddedTestProject(), "jobe");
    Assert.assertEquals(0, flow.getNode("joba").getLevel());
    Assert.assertEquals(1, flow.getNode("jobb").getLevel());
    Assert.assertEquals(1, flow.getNode("jobc").getLevel());
    Assert.assertEquals(1, flow.getNode("jobd").getLevel());
    Assert.assertEquals(2, flow.getNode("jobe").getLevel());
  }

}
