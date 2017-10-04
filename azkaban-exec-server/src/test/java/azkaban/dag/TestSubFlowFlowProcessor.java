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

import static java.util.Objects.requireNonNull;

public class TestSubFlowFlowProcessor implements FlowProcessor {

  private final DagService dagService;
  private final StatusChangeRecorder statusChangeRecorder;
  private Node node;


  public TestSubFlowFlowProcessor(final DagService dagService,
      final StatusChangeRecorder statusChangeRecorder
  ) {
    this.dagService = dagService;
    this.statusChangeRecorder = statusChangeRecorder;
  }


  @Override
  public void changeStatus(final Flow flow, final Status status) {
    System.out.println(flow);
    this.statusChangeRecorder.recordFlow(flow);
    requireNonNull(this.node);
    switch (status) {
      case SUCCESS:
        this.dagService.markJobSuccess(this.node);
        break;
      case FAILURE:
        this.dagService.failJob(this.node);
        break;
      case KILLED:
        //this.dagService.markJobKilled(this.node);
        break;
      default:
        // todo: save status
        break;
    }
  }

  /**
   * Sets the node that this subflow belongs.
   *
   * Can't pass this information in the constructor since it will cause a circular dependency
   * problem.
   *
   * @param node the node as part of the parent flow
   */
  public void setNode(final Node node) {
    this.node = node;
  }
}
