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

package azkaban.jobExecutor.utils;

public class JobExecutionException extends RuntimeException {

  private final static long serialVersionUID = 1;

  public JobExecutionException(String message) {
    super(message);
  }

  public JobExecutionException(Throwable cause) {
    super(cause);
  }

  public JobExecutionException(String message, Throwable cause) {
    super(message, cause);
  }

}
