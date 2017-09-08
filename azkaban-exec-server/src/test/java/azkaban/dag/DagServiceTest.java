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

package azkaban.dag;

import org.junit.Test;

public class DagServiceTest {

  @Test
  /**
   * Tests a DAG with one node which will run successfully.
   */
  public void OneNodeSuccess() throws Exception {
    final DagService dagService = new DagService();
    final NodeProcessor processor = new TestNodeProcessor(dagService);
    final Node aNode = new Node("a", processor);
    final FlowProcessor flowProcessor = new TestFlowProcessor();
    final Flow flow = new Flow("fa", flowProcessor);
    flow.addNode(aNode);
    dagService.startFlow(flow);
    Thread.sleep(1000000);
    dagService.shutdownAndAwaitTermination();
  }
}
