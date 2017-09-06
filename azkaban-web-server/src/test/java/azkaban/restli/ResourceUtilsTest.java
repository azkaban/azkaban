/*
 * Copyright 2016 LinkedIn Corp.
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

package azkaban.restli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import azkaban.fixture.MockResourceContext;
import org.junit.Test;

public class ResourceUtilsTest {

  @Test
  public void testWhenNoXForwardedForHeaderUseClientIp() {

    final String clientIp = "127.0.0.1:10000";
    final MockResourceContext ctx = MockResourceContext.getResourceContext(clientIp);
    assertNotNull(ctx);

    final String ip = ResourceUtils.getRealClientIpAddr(ctx);

    assertEquals(ip, "127.0.0.1");
  }

  @Test
  public void testWhenClientIpNoPort() {

    final String clientIp = "192.168.1.1";
    final MockResourceContext ctx = MockResourceContext.getResourceContext(clientIp);
    assertNotNull(ctx);

    final String ip = ResourceUtils.getRealClientIpAddr(ctx);

    assertEquals(ip, "192.168.1.1");
  }

  @Test
  public void testWhenXForwardedForHeaderUseHeader() {

    final String clientIp = "127.0.0.1:10000";
    final String upstreamIp = "192.168.1.1:10000";
    final MockResourceContext ctx = MockResourceContext
        .getResourceContextWithUpstream(clientIp, upstreamIp);
    assertNotNull(ctx);

    final String ip = ResourceUtils.getRealClientIpAddr(ctx);

    assertEquals(ip, "192.168.1.1");
  }

  @Test
  public void testWhenXForwardedForHeaderMultipleUpstreamsUseHeader() {

    final String clientIp = "127.0.0.1:10000";
    final String upstreamIp = "192.168.1.1:10000";
    final MockResourceContext ctx = MockResourceContext
        .getResourceContextWithMultipleUpstreams(clientIp, upstreamIp);
    assertNotNull(ctx);

    final String ip = ResourceUtils.getRealClientIpAddr(ctx);

    assertEquals(ip, "192.168.1.1");
  }
}
