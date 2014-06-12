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

import java.util.HashMap;
import java.util.Map;

import azkaban.user.User;

/**
 * Container for the session, mapping session id to user in map
 */
public class Session {
  private final User user;
  private final String sessionId;
  private final String ip;
  private Map<String, Object> sessionData = new HashMap<String, Object>();

  /**
   * Constructor for the session
   *
   * @param sessionId
   * @param user
   */
  public Session(String sessionId, User user, String ip) {
    this.user = user;
    this.sessionId = sessionId;
    this.ip = ip;
  }

  /**
   * Returns the User object
   *
   * @return
   */
  public User getUser() {
    return user;
  }

  /**
   * Returns the sessionId
   *
   * @return
   */
  public String getSessionId() {
    return sessionId;
  }

  public String getIp() {
    return ip;
  }

  public void setSessionData(String key, Object value) {
    sessionData.put(key, value);
  }

  public Object getSessionData(String key) {
    return sessionData.get(key);
  }
}
