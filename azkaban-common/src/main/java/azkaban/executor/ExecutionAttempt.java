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

import azkaban.utils.TypedMapWrapper;
import java.util.HashMap;
import java.util.Map;

public class ExecutionAttempt {

  public static final String ATTEMPT_PARAM = "attempt";
  public static final String STATUS_PARAM = "status";
  public static final String STARTTIME_PARAM = "startTime";
  public static final String ENDTIME_PARAM = "endTime";
  private final Status status;
  private int attempt = 0;
  private long startTime = -1;
  private long endTime = -1;

  public ExecutionAttempt(final int attempt, final ExecutableNode executable) {
    this.attempt = attempt;
    this.startTime = executable.getStartTime();
    this.endTime = executable.getEndTime();
    this.status = executable.getStatus();
  }

  public ExecutionAttempt(final int attempt, final long startTime, final long endTime,
      final Status status) {
    this.attempt = attempt;
    this.startTime = startTime;
    this.endTime = endTime;
    this.status = status;
  }

  public static ExecutionAttempt fromObject(final Object obj) {
    final Map<String, Object> map = (Map<String, Object>) obj;
    final TypedMapWrapper<String, Object> wrapper =
        new TypedMapWrapper<>(map);
    final int attempt = wrapper.getInt(ATTEMPT_PARAM);
    final long startTime = wrapper.getLong(STARTTIME_PARAM);
    final long endTime = wrapper.getLong(ENDTIME_PARAM);
    final Status status = Status.valueOf(wrapper.getString(STATUS_PARAM));

    return new ExecutionAttempt(attempt, startTime, endTime, status);
  }

  public long getStartTime() {
    return this.startTime;
  }

  public long getEndTime() {
    return this.endTime;
  }

  public Status getStatus() {
    return this.status;
  }

  public int getAttempt() {
    return this.attempt;
  }

  public Map<String, Object> toObject() {
    final Map<String, Object> attempt = new HashMap<>();
    attempt.put(ATTEMPT_PARAM, this.attempt);
    attempt.put(STARTTIME_PARAM, this.startTime);
    attempt.put(ENDTIME_PARAM, this.endTime);
    attempt.put(STATUS_PARAM, this.status.toString());
    return attempt;
  }
}
