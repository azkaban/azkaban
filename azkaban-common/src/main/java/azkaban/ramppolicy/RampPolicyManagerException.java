/*
 * Copyright 2019 LinkedIn Corp.
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
package azkaban.ramppolicy;


/**
 * Defined Exception Type for Ramp Policy Manager
 */
public class RampPolicyManagerException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public RampPolicyManagerException(final String message) {
    super(message);
  }

  public RampPolicyManagerException(final Throwable cause) {
    super(cause);
  }

  public RampPolicyManagerException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
