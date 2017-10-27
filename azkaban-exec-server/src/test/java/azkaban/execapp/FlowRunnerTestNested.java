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

package azkaban.execapp;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.Status;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FlowRunnerTestNested extends FlowRunnerTestBase {

  private FlowRunnerTestUtil testUtil;

  @Before
  public void setUp() throws Exception {
    this.testUtil = new FlowRunnerTestUtil("execnested", this.temporaryFolder);
  }

  @Test
  public void exec1Normal() throws Exception {
    final HashMap<String, String> flowProps = new HashMap<>();
    final FlowRunner runner = this.testUtil.createFromFlowMap("main", flowProps);
    final Map<String, ExecutableNode> nodeMap = new HashMap<>();
    createNodeMap(runner.getExecutableFlow(), nodeMap);
    final ExecutableFlow flow = runner.getExecutableFlow();

    FlowRunnerTestUtil.startThread(runner);
    assertStatus(flow, "do-echo-a:echo", Status.RUNNING);

    final Props echoAProps = PropsUtils.resolveProps(nodeMap.get("do-echo-a:echo").getInputProps());
    Assert.assertEquals("hello world", echoAProps.get("foo"));
    Assert.assertNull(echoAProps.get("foo.value"));

    final Props echoBProps = PropsUtils.resolveProps(nodeMap.get("do-echo-b:echo").getInputProps());
    Assert.assertEquals("foo bar", echoBProps.get("foo"));
    Assert.assertNull(echoBProps.get("foo.value"));
  }

  private void createNodeMap(final ExecutableFlowBase flow,
      final Map<String, ExecutableNode> nodeMap) {
    for (final ExecutableNode node : flow.getExecutableNodes()) {
      nodeMap.put(node.getNestedId(), node);

      if (node instanceof ExecutableFlowBase) {
        createNodeMap((ExecutableFlowBase) node, nodeMap);
      }
    }
  }
}
