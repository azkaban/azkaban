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
package azkaban.executor;


/**
 * Object to hold the status of the current executable Ramp
 */
public enum ExecutableRampStatus {
  // Identify the undetermined ramp status for the desired flow
  UNDETERMINED(""),
  // Identify the flow is qualified to ramp
  SELECTED("s"),
  // Identify the flow is still not ready to ramp due to the ramp policy checking
  UNSELECTED("u"),
  // This is a special Exceptional Status, Identify the flow/job will never be ramped regardless the ramp policy check
  BLACKLISTED("b"),
  // This is a special Exceptional Status, Identify the flow/job will always be ramped regardless the ramp policy check
  WHITELISTED("w"),
  // This is a special Exceptional Status, Identify the flow/job will be excluded from the ramp management
  // If the default Global Value has been set, the default Global Value will be applied,
  // otherwise, the customized dependency in workflow package will be used.
  EXCLUDED("x");

  private final String key;

  ExecutableRampStatus(String key) {
    this.key = key;
  }

  public static ExecutableRampStatus of(String key) {
    if (key.equalsIgnoreCase(ExecutableRampStatus.SELECTED.getKey())) {
      return ExecutableRampStatus.SELECTED;
    } else if (key.equalsIgnoreCase(ExecutableRampStatus.UNSELECTED.getKey())) {
      return ExecutableRampStatus.UNSELECTED;
    } else if (key.equalsIgnoreCase(ExecutableRampStatus.BLACKLISTED.getKey())) {
      return ExecutableRampStatus.BLACKLISTED;
    } else if (key.equalsIgnoreCase(ExecutableRampStatus.WHITELISTED.getKey())) {
      return ExecutableRampStatus.WHITELISTED;
    } else if (key.equalsIgnoreCase(ExecutableRampStatus.EXCLUDED.getKey())) {
      return ExecutableRampStatus.EXCLUDED;
    }
    return ExecutableRampStatus.UNDETERMINED;
  }

  public String getKey() {
    return key;
  }
}
