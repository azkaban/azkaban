/*
 * Copyright 2021 LinkedIn Corp.
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
package azkaban.executor.container.watch;

/**
 * Intended as a container for all the exceptions that can be thrown withing the kubernetes watch
 * implementation.
 * This can be used for 'wrapping' any checked exceptions used by the Watch listeners and is
 * open for extension in case there is a need for a more specific exceptions.
 */
public class AzkabanWatchException extends RuntimeException {

  public AzkabanWatchException() {
  }

  public AzkabanWatchException(String message) {
    super(message);
  }

  public AzkabanWatchException(String message, Throwable cause) {
    super(message, cause);
  }

  public AzkabanWatchException(Throwable cause) {
    super(cause);
  }
}
