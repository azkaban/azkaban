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

import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import org.junit.Test;

public final class SessionCacheTest {

  final String propsString = "{ \"session.time.to.live\": \"100\" }";

  SessionCache freshSessionCache() throws Exception {
    final Props props = PropsUtils.fromJSONString(propsString);
    return new SessionCache(props);
  }

  @Test
  public void SessionCacheHit() throws Exception {
    final SessionCache sessionCache = freshSessionCache();
    final Session session = new Session("TEST_SESSION_ID", new User("TEST_USER_HIT"),
        "123.12.12.123");
    sessionCache.addSession(session);
    assertThat(sessionCache.getSession("TEST_SESSION_ID")).isEqualTo(session);
  }

  @Test
  public void SessionCacheMiss() throws Exception {
    final SessionCache sessionCache = freshSessionCache();
    final Session session = new Session("TEST_SESSION_ID", new User("TEST_USER_MISS"),
        "123.12.12.123");
    sessionCache.addSession(session);
    Thread.sleep(200L);
    assertThat(sessionCache.getSession("TEST_SESSION_ID")).isNull();
  }

}
