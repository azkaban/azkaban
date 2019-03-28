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
package azkaban.server.session;

import azkaban.user.User;
import java.util.HashMap;
import java.util.Map;

/**
 * Container for the session, mapping session id to user in map
 */
public class Session {

  private final User user;
  private final String sessionId;
  @Deprecated
  private final String ip;
  private final Map<String, Object> sessionData = new HashMap<>();

  /**
   * Constructor for the session
   */
  public Session(final String sessionId, final User user, final String ip) {
    this.user = user;
    this.sessionId = sessionId;
    this.ip = ip;
  }

  /**
   * Returns the User object
   */
  public User getUser() {
    return this.user;
  }

  /**
   * Returns the sessionId
   */
  public String getSessionId() {
    return this.sessionId;
  }

  @Deprecated
  public String getIp() {
    return this.ip;
  }

  public void setSessionData(final String key, final Object value) {
    this.sessionData.put(key, value);
  }

  public Object getSessionData(final String key) {
    return this.sessionData.get(key);
  }
}
