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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Cache;

import java.util.concurrent.TimeUnit;

import azkaban.utils.Props;

/**
 * Cache for web session.
 *
 * The following global azkaban properties can be used: max.num.sessions - used
 * to determine the number of live sessions that azkaban will handle. Default is
 * 10000 session.time.to.live -Number of seconds before session expires. Default
 * set to 1 days.
 */
public class SessionCache {
  private static final int MAX_NUM_SESSIONS = 10000;
  private static final long SESSION_TIME_TO_LIVE = 24 * 60 * 60 * 1000L;

  // private CacheManager manager = CacheManager.create();
  private Cache<String, Session> cache;

  /**
   * Constructor taking global props.
   *
   * @param props
   */
  public SessionCache(Props props) {
    cache = CacheBuilder.newBuilder()
        .maximumSize(props.getInt("max.num.sessions", MAX_NUM_SESSIONS))
        .expireAfterAccess(
            props.getLong("session.time.to.live", SESSION_TIME_TO_LIVE),
            TimeUnit.MILLISECONDS)
        .build();
  }

  /**
   * Returns the cached session using the session id.
   *
   * @param sessionId
   * @return
   */
  public Session getSession(String sessionId) {
    Session elem = cache.getIfPresent(sessionId);
    return elem;
  }

  /**
   * Adds a session to the cache. Accessible through the session ID.
   *
   * @param id
   * @param session
   */
  public void addSession(Session session) {
    cache.put(session.getSessionId(), session);
  }

  /**
   * Removes the session from the cache.
   *
   * @param id
   * @return
   */
  public void removeSession(String id) {
    cache.invalidate(id);
  }
}
