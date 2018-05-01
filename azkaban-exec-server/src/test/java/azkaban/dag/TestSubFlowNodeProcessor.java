/*
 * Copyright 2018 LinkedIn Corp.
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

public class TestSubFlowNodeProcessor implements NodeProcessor {

  private final DagService dagService;
  private final StatusChangeRecorder statusChangeRecorder;
  private final Flow flow;


  TestSubFlowNodeProcessor(final DagService dagService,
      final StatusChangeRecorder statusChangeRecorder,
      final Flow flow
  ) {
    this.dagService = dagService;
    this.statusChangeRecorder = statusChangeRecorder;
    this.flow = flow;
  }


  @Override
  public void changeStatus(final Node node, final Status status) {
    System.out.println(node);
    this.statusChangeRecorder.recordNode(node);

    switch (status) {
      case RUNNING:
        this.dagService.startFlow(this.flow);
        break;
      case KILLING:
        this.dagService.killFlow(this.flow);
        break;
      default:
        // todo: save status
        break;
    }


  }
}
