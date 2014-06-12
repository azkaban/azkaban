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

package azkaban.trigger;

public enum TriggerStatus {
  READY(10), PAUSED(20), EXPIRED(30);

  private int numVal;

  TriggerStatus(int numVal) {
    this.numVal = numVal;
  }

  public int getNumVal() {
    return numVal;
  }

  public static TriggerStatus fromInteger(int x) {
    switch (x) {
    case 10:
      return READY;
    case 20:
      return PAUSED;
    case 30:
      return EXPIRED;
    default:
      return READY;
    }
  }

}
