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

import azkaban.utils.Pair;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public enum Status {
  READY(10),
  DISPATCHING (15),
  PREPARING(20),
  RUNNING(30),
  PAUSED(40),
  SUCCEEDED(50),
  KILLING(55),
  KILLED(60),
  // EXECUTION_STOPPED refers to a terminal flow status due to crashed executor/container
  EXECUTION_STOPPED(65),
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

  private static final String SUBMIT_TIME = "submit_time";
  private static final String START_TIME = "start_time";
  private static final String UPDATE_TIME = "update_time";
  // Defines the validity duration associated with certain statuses from the
  // submit/start/update time.
  public static final ImmutableMap<Status, Pair<Duration, String>> validityMap = new Builder<Status,
      Pair<Duration, String>>()
      .put(DISPATCHING, new Pair<>(Duration.ofMinutes(10), SUBMIT_TIME))
      .put(PREPARING, new Pair<>(Duration.ofMinutes(15), SUBMIT_TIME))
      .put(RUNNING, new Pair<>(Duration.ofDays(10), START_TIME))
      .put(PAUSED, new Pair<>(Duration.ofDays(10), START_TIME))
      .put(KILLING, new Pair<>(Duration.ofMinutes(15), UPDATE_TIME))
      .put(EXECUTION_STOPPED, new Pair<>(Duration.ofMinutes(15), UPDATE_TIME))
      .put(FAILED_FINISHING, new Pair<>(Duration.ofDays(10), START_TIME))
      .build();

  public static final Set<Status> nonFinishingStatusAfterFlowStartsSet = new TreeSet<>(
      Arrays.asList(Status.RUNNING, Status.QUEUED, Status.PAUSED, Status.FAILED_FINISHING));

  private final int numVal;

  Status(final int numVal) {
    this.numVal = numVal;
  }

  public static Status fromInteger(final int x) {
    return numValMap.getOrDefault(x, READY);
  }

  public static boolean isStatusFinished(final Status status) {
    switch (status) {
      case EXECUTION_STOPPED:
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

  public static boolean isStatusFailed(final Status status) {
    switch (status) {
      case FAILED:
      case KILLED:
      case CANCELLED:
        return true;
      default:
        return false;
    }
  }

  public static boolean isStatusSucceeded(final Status status) {
    switch (status) {
      case SUCCEEDED:
      case FAILED_SUCCEEDED:
      case SKIPPED:
        return true;
      default:
        return false;
    }
  }

  public int getNumVal() {
    return this.numVal;
  }
}
