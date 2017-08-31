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

package azkaban.executor;

import java.util.Date;

/**
 * Class to represent events on Azkaban executors
 *
 * @author gaggarwa
 */
public class ExecutorLogEvent {

  private final int executorId;
  private final String user;
  private final Date time;
  private final EventType type;
  private final String message;

  public ExecutorLogEvent(final int executorId, final String user, final Date time,
      final EventType type, final String message) {
    this.executorId = executorId;
    this.user = user;
    this.time = time;
    this.type = type;
    this.message = message;
  }

  public int getExecutorId() {
    return this.executorId;
  }

  public String getUser() {
    return this.user;
  }

  public Date getTime() {
    return this.time;
  }

  public EventType getType() {
    return this.type;
  }

  public String getMessage() {
    return this.message;
  }

  /**
   * Log event type messages. Do not change the numeric representation of each enum. Only represent
   * from 0 to 255 different codes.
   */
  public enum EventType {
    ERROR(128), HOST_UPDATE(1), PORT_UPDATE(2), ACTIVATION(3), INACTIVATION(4),
    CREATED(5);

    private final int numVal;

    EventType(final int numVal) {
      this.numVal = numVal;
    }

    public static EventType fromInteger(final int x)
        throws IllegalArgumentException {
      switch (x) {
        case 1:
          return HOST_UPDATE;
        case 2:
          return PORT_UPDATE;
        case 3:
          return ACTIVATION;
        case 4:
          return INACTIVATION;
        case 5:
          return CREATED;
        case 128:
          return ERROR;
        default:
          throw new IllegalArgumentException(String.format(
              "inalid status code %d", x));
      }
    }

    public int getNumVal() {
      return this.numVal;
    }
  }
}
