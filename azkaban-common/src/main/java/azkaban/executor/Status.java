/*
 * Copyright 2014 LinkedIn Corp.
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

public enum Status {
  READY(10),
  PREPARING(20),
  RUNNING(30),
  PAUSED(40),
  SUCCEEDED(50),
  KILLED(60),
  FAILED(70),
  FAILED_FINISHING(80),
  SKIPPED(90),
  DISABLED(100),
  QUEUED(110),
  FAILED_SUCCEEDED(120),
  CANCELLED(130);

  private int numVal;

  Status(int numVal) {
    this.numVal = numVal;
  }

  public int getNumVal() {
    return numVal;
  }

  public static Status fromInteger(int x) {
    switch (x) {
    case 10:
      return READY;
    case 20:
      return PREPARING;
    case 30:
      return RUNNING;
    case 40:
      return PAUSED;
    case 50:
      return SUCCEEDED;
    case 60:
      return KILLED;
    case 70:
      return FAILED;
    case 80:
      return FAILED_FINISHING;
    case 90:
      return SKIPPED;
    case 100:
      return DISABLED;
    case 110:
      return QUEUED;
    case 120:
      return FAILED_SUCCEEDED;
    case 130:
      return CANCELLED;
    default:
      return READY;
    }
  }

  public static boolean isStatusFinished(Status status) {
    switch (status) {
    case FAILED:
    case KILLED:
    case SUCCEEDED:
    case SKIPPED:
    case FAILED_SUCCEEDED:
    case CANCELLED:
      return true;
    default:
      return false;
    }
  }

  public static boolean isStatusRunning(Status status) {
    switch (status) {
    case RUNNING:
    case FAILED_FINISHING:
    case QUEUED:
      return true;
    default:
      return false;
    }
  }
}
