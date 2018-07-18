/*
* Copyright 2018 LinkedIn Corp.
*
* Licensed under the Apache License, Version 2.0 (the “License”); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/
package azkaban.flow;

public enum ConditionOnJobStatus {
  ALL_SUCCESS("all_success"),
  ALL_FAILED("all_failed"),
  ALL_DONE("all_done"),
  ONE_FAILED("one_failed"),
  ONE_SUCCESS("one_success"),
  ONE_FAILED_ALL_DONE("one_failed_all_done"),
  ONE_SUCCESS_ALL_DONE("one_success_all_done");

  private final String condition;

  ConditionOnJobStatus(final String condition) {
    this.condition = condition;
  }

  public static ConditionOnJobStatus fromString(final String condition) {
    for (final ConditionOnJobStatus conditionOnJobStatus : ConditionOnJobStatus.values()) {
      if (conditionOnJobStatus.condition.equalsIgnoreCase(condition)) {
        return conditionOnJobStatus;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return this.condition;
  }

}
