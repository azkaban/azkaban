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

package azkaban.executor;

public class ExecutionReference {
  private final int execId;
  private Executor executor;
  private long updateTime;
  private long nextCheckTime = -1;
  private int numErrors = 0;


  public ExecutionReference(int execId) {
    this.execId = execId;
  }

  public ExecutionReference(int execId, Executor executor) {
    if (executor == null) {
      throw new IllegalArgumentException(String.format(
        "Executor cannot be null for exec id: %d ExecutionReference", execId));
    }
    this.execId = execId;
    this.executor = executor;
  }

  public void setUpdateTime(long updateTime) {
    this.updateTime = updateTime;
  }

  public void setNextCheckTime(long nextCheckTime) {
    this.nextCheckTime = nextCheckTime;
  }

  public long getUpdateTime() {
    return updateTime;
  }

  public long getNextCheckTime() {
    return nextCheckTime;
  }

  public int getExecId() {
    return execId;
  }

  public String getHost() {
    return executor.getHost();
  }

  public int getPort() {
    return executor.getPort();
  }

  public int getNumErrors() {
    return numErrors;
  }

  public void setNumErrors(int numErrors) {
    this.numErrors = numErrors;
  }

  public void setExecutor(Executor executor) {
    this.executor = executor;
  }

  public Executor getExecutor() {
    return executor;
  }
}