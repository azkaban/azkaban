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

import java.util.HashMap;
import java.util.Map;

import azkaban.utils.TypedMapWrapper;

public class ExecutionAttempt {
  public static final String ATTEMPT_PARAM = "attempt";
  public static final String STATUS_PARAM = "status";
  public static final String STARTTIME_PARAM = "startTime";
  public static final String ENDTIME_PARAM = "endTime";

  private int attempt = 0;
  private long startTime = -1;
  private long endTime = -1;
  private Status status;

  public ExecutionAttempt(int attempt, ExecutableNode executable) {
    this.attempt = attempt;
    this.startTime = executable.getStartTime();
    this.endTime = executable.getEndTime();
    this.status = executable.getStatus();
  }

  public ExecutionAttempt(int attempt, long startTime, long endTime,
      Status status) {
    this.attempt = attempt;
    this.startTime = startTime;
    this.endTime = endTime;
    this.status = status;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public Status getStatus() {
    return status;
  }

  public int getAttempt() {
    return attempt;
  }

  public static ExecutionAttempt fromObject(Object obj) {
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) obj;
    TypedMapWrapper<String, Object> wrapper =
        new TypedMapWrapper<String, Object>(map);
    int attempt = wrapper.getInt(ATTEMPT_PARAM);
    long startTime = wrapper.getLong(STARTTIME_PARAM);
    long endTime = wrapper.getLong(ENDTIME_PARAM);
    Status status = Status.valueOf(wrapper.getString(STATUS_PARAM));

    return new ExecutionAttempt(attempt, startTime, endTime, status);
  }

  public Map<String, Object> toObject() {
    HashMap<String, Object> attempts = new HashMap<String, Object>();
    attempts.put(ATTEMPT_PARAM, attempt);
    attempts.put(STARTTIME_PARAM, startTime);
    attempts.put(ENDTIME_PARAM, endTime);
    attempts.put(STATUS_PARAM, status.toString());
    return attempts;
  }
}
