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

package azkaban.flowtrigger;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

/**
 * Represents status for trigger/dependency
 */
public enum Status {
  RUNNING, // dependency instance is running
  SUCCEEDED, // dependency instance succeeds
  CANCELLED, // dependency instance is cancelled
  CANCELLING; // dependency instance is being cancelled

  public static boolean isDone(final Status status) {
    final Set<Status> terminalStatus = ImmutableSet.of(SUCCEEDED, CANCELLED);
    return terminalStatus.contains(status);
  }
}
