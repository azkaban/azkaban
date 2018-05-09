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

import static java.util.Objects.requireNonNull;

/**
 * A DAG processor that brides the sub DAG and the parent DAG.
 */
public class TestSubDagProcessor implements DagProcessor {

  private final DagService dagService;
  private final StatusChangeRecorder statusChangeRecorder;
  private Node node;


  TestSubDagProcessor(final DagService dagService,
      final StatusChangeRecorder statusChangeRecorder
  ) {
    this.dagService = dagService;
    this.statusChangeRecorder = statusChangeRecorder;
  }


  /**
   * Transfers the node state in the parent DAG when the sub DAG status changes.
   *
   * @param dag the dag to change
   * @param status the new status
   */
  @Override
  public void changeStatus(final Dag dag, final Status status) {
    System.out.println(dag);
    this.statusChangeRecorder.recordDag(dag);
    requireNonNull(this.node, "Node for the subDag in the parent DAG can't be null.");
    switch (status) {
      case SUCCESS:
        this.dagService.markNodeSuccess(this.node);
        break;
      case FAILURE:
        this.dagService.markNodeFailed(this.node);
        break;
      default:
        break;
    }
  }

  /**
   * Sets the node that this subflow belongs.
   * <p>
   * Can't pass this information in the constructor since it will cause a circular dependency
   * problem.
   *
   * @param node the node as part of the parent flow
   */
  public void setNode(final Node node) {
    this.node = node;
  }
}
