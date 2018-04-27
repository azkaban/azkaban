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

package azkaban.flowtrigger.database;

import azkaban.flowtrigger.DependencyInstance;
import azkaban.flowtrigger.TriggerInstance;
import java.util.Collection;

public interface FlowTriggerInstanceLoader {

  /**
   * Upload a trigger instance into db
   */
  void uploadTriggerInstance(TriggerInstance triggerInstance);

  /**
   * Update dependency status, cancellation cause and end time
   */
  void updateDependencyExecutionStatus(DependencyInstance depInst);

  /**
   * Retrieve trigger instances not in done state(cancelling, running, or succeeded but associated
   * flow hasn't been triggered yet). This is used when recovering unfinished
   * trigger instance during web server restarts.
   */
  Collection<TriggerInstance> getIncompleteTriggerInstances();

  /**
   * Update associated flow execution id for a trigger instance. This will be called when a trigger
   * instance successfully starts a flow.
   */
  void updateAssociatedFlowExecId(TriggerInstance triggerInst);

  /**
   * Retrieve recently finished trigger instances.
   *
   * @param limit number of trigger instances to retrieve
   */
  Collection<TriggerInstance> getRecentlyFinished(int limit);

  /**
   * Retrieve running trigger instances.
   */
  Collection<TriggerInstance> getRunning();

  TriggerInstance getTriggerInstanceById(String triggerInstanceId);

  TriggerInstance getTriggerInstanceByFlowExecId(int execId);

  Collection<TriggerInstance> getTriggerInstances(int projectId, String flowId, int from, int
      length);

  /**
   * Delete trigger instances whose endtime is older than the timestamp
   *
   * @return number of deleted rows(dependency instances) ;
   */
  int deleteCompleteTriggerExecutionFinishingOlderThan(long timestamp);
}
