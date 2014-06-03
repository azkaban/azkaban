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

public class ExecutorManagerException extends Exception {
  public enum Reason {
    SkippedExecution
  }

  private static final long serialVersionUID = 1L;
  private ExecutableFlow flow = null;
  private Reason reason = null;

  public ExecutorManagerException(Exception e) {
    super(e);
  }

  public ExecutorManagerException(String message) {
    super(message);
  }

  public ExecutorManagerException(String message, ExecutableFlow flow) {
    super(message);
    this.flow = flow;
  }

  public ExecutorManagerException(String message, Reason reason) {
    super(message);
    this.reason = reason;
  }

  public ExecutorManagerException(String message, Throwable cause) {
    super(message, cause);
  }

  public ExecutableFlow getExecutableFlow() {
    return flow;
  }

  public Reason getReason() {
    return reason;
  }
}
