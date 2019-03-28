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

import azkaban.Constants.ConfigurationKeys;
import azkaban.utils.Props;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

/**
 * Cache for web session.
 *
 * The following global Azkaban properties are used:
 * <ul>
 *   <li>{@code max.num.sessions} - number of live sessions that Azkaban handles, default is 10000
 *   <li>{@code session.time.to.live} - number of milliseconds before the session expires,
 *   default 36000000 ms, i.e. 10 hours.
 * </ul>
 */
public class SessionCache {

  private static final int MAX_NUM_SESSIONS = 10000;
  private static final long DEFAULT_SESSION_TIME_TO_LIVE = 10 * 60 * 60 * 1000L; // 10 hours

  private final Cache<String, Session> cache;

  private final long effectiveSessionTimeToLive;

  /**
   * Constructor taking global props.
   */
  @Inject
  public SessionCache(final Props props) {
    this.effectiveSessionTimeToLive = props.getLong(ConfigurationKeys.SESSION_TIME_TO_LIVE,
        DEFAULT_SESSION_TIME_TO_LIVE);
    this.cache = CacheBuilder.newBuilder()
        .maximumSize(props.getInt("max.num.sessions", MAX_NUM_SESSIONS))
        .expireAfterAccess(this.effectiveSessionTimeToLive, TimeUnit.MILLISECONDS)
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
   * Returns the approximate number of sessions currently be kept.
   */
  public long getSessionCount() {
    return this.cache.size();
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
