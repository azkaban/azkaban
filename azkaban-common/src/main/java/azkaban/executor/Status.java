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

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;

public enum Status {
  READY(10),
  PREPARING(20),
  RUNNING(30),
  PAUSED(40),
  SUCCEEDED(50),
  KILLING(55),
  KILLED(60),
  FAILED(70),
  FAILED_FINISHING(80),
  SKIPPED(90),
  DISABLED(100),
  QUEUED(110),
  FAILED_SUCCEEDED(120),
  CANCELLED(125);
  // status is TINYINT in DB and the value ranges from -128 to 127

  private static final ImmutableMap<Integer, Status> numValMap = Arrays.stream(Status.values())
      .collect(ImmutableMap.toImmutableMap(status -> status.getNumVal(), status -> status));

  private final int numVal;

  Status(final int numVal) {
    this.numVal = numVal;
  }

  public static Status fromInteger(final int x) {
    return numValMap.getOrDefault(x, READY);
  }

  public static boolean isStatusFinished(final Status status) {
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

  public static boolean isStatusRunning(final Status status) {
    switch (status) {
      case RUNNING:
      case FAILED_FINISHING:
      case QUEUED:
        return true;
      default:
        return false;
    }
  }

  public int getNumVal() {
    return this.numVal;
  }
}
