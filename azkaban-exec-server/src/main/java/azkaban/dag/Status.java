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

package azkaban.dag;

import com.google.common.collect.ImmutableSet;

public enum Status {
  READY, // ready to run
  DISABLED, // disabled by users. Treat as the node has the status of success
  BLOCKED, // temporarily blocked. Need to be unblocked by another external event
  RUNNING,
  SUCCESS,
  FAILURE,

  // doesn't run because one of the nodes it depends on fails or is killed. Applies to a node only.
  CANCELED,
  KILLING, // in the process of killing a running job
  KILLED; // explicitly killed by a user

  // The states that will not transition to other states
  static final ImmutableSet<Status> TERMINAL_STATES = ImmutableSet.of(DISABLED, SUCCESS, FAILURE,
      CANCELED, KILLED);

  public boolean isTerminal() {
    return TERMINAL_STATES.contains(this);
  }

  // The states that are considered as success effectively
  private static final ImmutableSet<Status> EFFECTIVE_SUCCESS_STATES = ImmutableSet.of(DISABLED,
      SUCCESS);

  boolean isSuccessEffectively() {
    return EFFECTIVE_SUCCESS_STATES.contains(this);
  }

  // The states that are possible before a node ever starts to run or be killed or canceled
  private static final ImmutableSet<Status> PRE_RUN_STATES = ImmutableSet
      .of(DISABLED, READY, BLOCKED);

  boolean isPreRunState() {
    return PRE_RUN_STATES.contains(this);
  }
}
