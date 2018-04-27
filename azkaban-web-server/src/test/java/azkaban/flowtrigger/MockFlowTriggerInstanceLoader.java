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

package azkaban.flowtrigger;

import azkaban.flowtrigger.database.FlowTriggerInstanceLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class MockFlowTriggerInstanceLoader implements FlowTriggerInstanceLoader {

  private final List<TriggerInstance> triggerInstances = Collections.synchronizedList(new
      ArrayList<TriggerInstance>());

  public void clear() {
    this.triggerInstances.clear();
  }

  @Override
  public void uploadTriggerInstance(final TriggerInstance triggerInstance) {
    this.triggerInstances.add(triggerInstance);
  }

  @Override
  public void updateDependencyExecutionStatus(final DependencyInstance depInst) {
    for (final TriggerInstance inst : this.triggerInstances) {
      if (inst.getId().equals(depInst.getTriggerInstance().getId())) {
        for (final DependencyInstance dep : inst.getDepInstances()) {
          if (dep.getDepName().equals(depInst.getDepName())) {
            dep.setEndTime(depInst.getEndTime());
            dep.setStatus(depInst.getStatus());
            dep.setCancellationCause(depInst.getCancellationCause());
            break;
          }
        }
        break;
      }
    }
  }

  @Override
  public Collection<TriggerInstance> getIncompleteTriggerInstances() {
    final List<TriggerInstance> res = new ArrayList<>();
    for (final TriggerInstance inst : this.triggerInstances) {
      if (inst.getStatus() == Status.CANCELLING || inst.getStatus() == Status.RUNNING) {
        res.add(inst);
      }
    }
    return res;
  }

  @Override
  public void updateAssociatedFlowExecId(final TriggerInstance triggerInst) {
    for (final TriggerInstance inst : this.triggerInstances) {
      if (triggerInst.getId().equals(triggerInst.getId())) {
        inst.setFlowExecId(triggerInst.getFlowExecId());
        break;
      }
    }
  }

  @Override
  public Collection<TriggerInstance> getRecentlyFinished(final int limit) {
    final List<TriggerInstance> res = new ArrayList<>();
    for (final TriggerInstance inst : this.triggerInstances) {
      if (Status.isDone(inst.getStatus())) {
        res.add(inst);
      }
    }
    return res;
  }

  @Override
  public Collection<TriggerInstance> getRunning() {
    final List<TriggerInstance> res = new ArrayList<>();
    for (final TriggerInstance inst : this.triggerInstances) {
      if (!Status.isDone(inst.getStatus())) {
        res.add(inst);
      }
    }
    return res;
  }

  @Override
  public TriggerInstance getTriggerInstanceById(final String triggerInstanceId) {
    for (final TriggerInstance inst : this.triggerInstances) {
      if (inst.getId().equals(triggerInstanceId)) {
        return inst;
      }
    }
    return null;
  }

  @Override
  public TriggerInstance getTriggerInstanceByFlowExecId(final int execId) {
    for (final TriggerInstance inst : this.triggerInstances) {
      if (inst.getFlowExecId() == execId) {
        return inst;
      }
    }
    return null;
  }

  @Override
  public Collection<TriggerInstance> getTriggerInstances(final int projectId, final String flowId,
      final int from, final int length) {
    throw new UnsupportedOperationException("Not Yet Implemented");
  }

  @Override
  public int deleteCompleteTriggerExecutionFinishingOlderThan(final long timestamp) {
    int deleted = 0;
    for (final Iterator<TriggerInstance> iterator = this.triggerInstances.iterator();
        iterator.hasNext(); ) {
      final TriggerInstance inst = iterator.next();
      if ((inst.getEndTime() <= timestamp) && ((inst.getStatus() == Status.CANCELLED) || ((inst
          .getStatus() == Status.SUCCEEDED) && (inst.getFlowExecId() != -1)))) {
        iterator.remove();
        deleted++;
      }
    }
    return deleted;
  }
}
