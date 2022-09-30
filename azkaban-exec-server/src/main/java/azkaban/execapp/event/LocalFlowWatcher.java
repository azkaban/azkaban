/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.execapp.event;

import azkaban.event.Event;
import azkaban.event.EventData;
import azkaban.event.EventListener;
import azkaban.execapp.FlowRunner;
import azkaban.execapp.JobRunner;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.spi.EventType;

public class LocalFlowWatcher extends FlowWatcher {

  private final LocalFlowWatcherListener watcherListener;
  private FlowRunner runner;
  private boolean isShutdown = false;

  public LocalFlowWatcher(final FlowRunner runner) {
    super(runner.getExecutableFlow().getExecutionId());
    super.setFlow(runner.getExecutableFlow());

    this.watcherListener = new LocalFlowWatcherListener();
    this.runner = runner;
    runner.addListener(this.watcherListener);
  }

  @Override
  public void stopWatcher() {
    // Just freeing stuff
    if (this.isShutdown) {
      return;
    }

    this.isShutdown = true;
    this.runner.removeListener(this.watcherListener);
    this.runner = null;

    getLogger().info("Stopping watcher, and unblocking pipeline");
    super.unblockAllWatches();
  }

  public class LocalFlowWatcherListener implements EventListener<Event> {

    @Override
    public void handleEvent(final Event event) {
      if (event.getType() == EventType.JOB_FINISHED) {
        if (event.getRunner() instanceof FlowRunner) {
          // The flow runner will finish a job without it running
          final EventData eventData = event.getData();
          if (eventData.getNestedId() != null) {
            handleJobStatusChange(eventData.getNestedId(), eventData.getStatus());
          }
        } else if (event.getRunner() instanceof JobRunner) {
          // A job runner is finished
          final JobRunner runner = (JobRunner) event.getRunner();
          final ExecutableNode node = runner.getNode();
          getLogger().info(node + " looks like " + node.getStatus());
          handleJobStatusChange(node.getNestedId(), node.getStatus());
        }
      } else if (event.getData().getNode() instanceof ExecutableFlow && event.getType() == EventType.FLOW_FINISHED) {
        stopWatcher();
      }
    }
  }
}
