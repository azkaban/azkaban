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

import azkaban.utils.Props;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

/**
 * Cache for web session.
 *
 * The following global azkaban properties can be used: max.num.sessions - used to determine the
 * number of live sessions that azkaban will handle. Default is 10000 session.time.to.live -Number
 * of seconds before session expires. Default set to 10 hours.
 */
public class SessionCache {

  private static final int MAX_NUM_SESSIONS = 10000;
  private static final long SESSION_TIME_TO_LIVE = 10 * 60 * 60 * 1000L;

  // private CacheManager manager = CacheManager.create();
  private final Cache<String, Session> cache;

  /**
   * Constructor taking global props.
   */
  @Inject
  public SessionCache(final Props props) {
    this.cache = CacheBuilder.newBuilder()
        .maximumSize(props.getInt("max.num.sessions", MAX_NUM_SESSIONS))
        .expireAfterAccess(
            props.getLong("session.time.to.live", SESSION_TIME_TO_LIVE),
            TimeUnit.MILLISECONDS)
        .build();
  }

  /**
   * Returns the cached session using the session id.
   */
  public Session getSession(final String sessionId) {
    final Session elem = this.cache.getIfPresent(sessionId);
    return elem;
  }

  /**
   * Adds a session to the cache. Accessible through the session ID.
   */
  public void addSession(final Session session) {
    this.cache.put(session.getSessionId(), session);
  }

  /**
   * Removes the session from the cache.
   */
  public void removeSession(final String id) {
    this.cache.invalidate(id);
  }
}
