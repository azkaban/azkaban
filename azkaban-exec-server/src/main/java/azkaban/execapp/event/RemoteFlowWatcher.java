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

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import java.util.ArrayList;
import java.util.Map;


public class RemoteFlowWatcher extends FlowWatcher {

  private final static long CHECK_INTERVAL_MS = 60000; // 60 * 1000

  private int execId;
  private ExecutorLoader loader;
  private ExecutableFlow flow;
  private RemoteUpdaterThread thread;
  private boolean isShutdown = false;

  // Every minute
  private long checkIntervalMs = CHECK_INTERVAL_MS;

  public RemoteFlowWatcher(final int execId, final ExecutorLoader loader) {
    this(execId, loader, CHECK_INTERVAL_MS);
  }

  public RemoteFlowWatcher(final int execId, final ExecutorLoader loader, final long interval) {
    super(execId);
    this.checkIntervalMs = interval;

    try {
      this.flow = loader.fetchExecutableFlow(execId);
    } catch (final ExecutorManagerException e) {
      return;
    }

    super.setFlow(this.flow);
    this.loader = loader;
    this.execId = execId;
    if (this.flow != null) {
      this.thread = new RemoteUpdaterThread();
      this.thread.setName("Remote-watcher-flow-" + execId);
      this.thread.start();
    }
  }

  @Override
  public synchronized void stopWatcher() {
    if (this.isShutdown) {
      return;
    }
    this.isShutdown = true;
    if (this.thread != null) {
      this.thread.interrupt();
    }
    super.unblockAllWatches();
    this.loader = null;
    this.flow = null;
  }

  private class RemoteUpdaterThread extends Thread {

    @Override
    public void run() {
      do {
        ExecutableFlow updateFlow = null;
        try {
          updateFlow = RemoteFlowWatcher.this.loader.fetchExecutableFlow(
              RemoteFlowWatcher.this.execId);
        } catch (final ExecutorManagerException e) {
          e.printStackTrace();
          RemoteFlowWatcher.this.isShutdown = true;
        }

        long updateTime = 0;
        if (RemoteFlowWatcher.this.flow == null) {
          RemoteFlowWatcher.this.flow = updateFlow;
        } else {
          final Map<String, Object> updateData =
              updateFlow.toUpdateObject(updateTime);
          final ArrayList<ExecutableNode> updatedNodes =
              new ArrayList<>();
          RemoteFlowWatcher.this.flow.applyUpdateObject(updateData, updatedNodes);

          RemoteFlowWatcher.this.flow.setStatus(updateFlow.getStatus());
          RemoteFlowWatcher.this.flow.setEndTime(updateFlow.getEndTime());
          RemoteFlowWatcher.this.flow.setUpdateTime(updateFlow.getUpdateTime());

          for (final ExecutableNode node : updatedNodes) {
            handleJobStatusChange(node.getNestedId(), node.getStatus());
          }

          updateTime = RemoteFlowWatcher.this.flow.getUpdateTime();
        }

        if (Status.isStatusFinished(RemoteFlowWatcher.this.flow.getStatus())) {
          RemoteFlowWatcher.this.isShutdown = true;
        } else {
          synchronized (this) {
            try {
              wait(RemoteFlowWatcher.this.checkIntervalMs);
            } catch (final InterruptedException e) {
            }
          }
        }
      } while (!RemoteFlowWatcher.this.isShutdown);
    }

  }
}
