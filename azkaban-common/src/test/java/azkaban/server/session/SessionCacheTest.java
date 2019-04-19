/*
 * Copyright 2018 LinkedIn Corp.
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

import static org.assertj.core.api.Assertions.assertThat;

import azkaban.Constants.ConfigurationKeys;
import azkaban.user.User;
import azkaban.utils.Props;
import org.junit.Test;

public final class SessionCacheTest {

  final long shortTTL = 100L;
  final long longTTL = 10 * 10 * 100L;

  SessionCache createSessionCacheWithTTL(long ttl) throws Exception {
    Props props = new Props();
    props.put(ConfigurationKeys.SESSION_TIME_TO_LIVE, ttl);
    return new SessionCache(props);
  }

  @Test
  public void SessionCacheHit() throws Exception {
    final SessionCache sessionCache = createSessionCacheWithTTL(longTTL);
    final Session session = new Session("TEST_SESSION_ID", new User("TEST_USER_HIT"),
        "123.12.12.123");
    sessionCache.addSession(session);
    assertThat(sessionCache.getSession("TEST_SESSION_ID")).isEqualTo(session);
  }

  @Test
  public void SessionCacheCount() throws Exception {
    final SessionCache sessionCache = createSessionCacheWithTTL(longTTL);
    final Session session = new Session("TEST_SESSION_ID", new User("TEST_USER_HIT"),
        "123.12.12.123");
    assertThat(sessionCache.getSessionCount()).isEqualTo(0);
    sessionCache.addSession(session);
    assertThat(sessionCache.getSessionCount()).isEqualTo(1);
    sessionCache.removeSession(session.getSessionId());
    assertThat(sessionCache.getSessionCount()).isEqualTo(0);
  }

  @Test
  public void SessionCacheFindByIP() throws Exception {
    final SessionCache sessionCache = createSessionCacheWithTTL(longTTL);
    final String ip = "123.12.12.123";
    final String id1 = "TEST_ID1";
    final String id2 = "TEST_ID2";
    sessionCache.addSession(new Session(id1, new User("TEST_USER_HIT"), ip));
    sessionCache.addSession(new Session(id2, new User("TEST_USER_HIT"), ip));

    assertThat(sessionCache.findSessionsByIP(ip)).hasSize(2);
    assertThat(sessionCache.findSessionsByIP("0")).isEmpty();
    sessionCache.removeSession(id1);
    assertThat(sessionCache.findSessionsByIP(ip)).hasSize(1);
    sessionCache.removeSession(id2);
    assertThat(sessionCache.findSessionsByIP(ip)).isEmpty();
  }

  @Test
  public void SessionCacheMiss() throws Exception {
    final SessionCache sessionCache = createSessionCacheWithTTL(shortTTL);
    final Session session = new Session("TEST_SESSION_ID", new User("TEST_USER_MISS"),
        "123.12.12.123");
    sessionCache.addSession(session);
    Thread.sleep(200L);
    assertThat(sessionCache.getSession("TEST_SESSION_ID")).isNull();
  }

  @Test
  public void SessionCacheNoExpired() throws Exception {
    final SessionCache sessionCache = createSessionCacheWithTTL(longTTL);
    final Session session = new Session("TEST_SESSION_ID", new User("TEST_USER_MISS"),
            "123.12.12.123");
    sessionCache.addSession(session);
    Thread.sleep(200L);
    assertThat(sessionCache.getSession("TEST_SESSION_ID")).isEqualTo(session);
  }
}
