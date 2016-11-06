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

package azkaban.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test class for azkaban.utils.WebUtils
 */
public class WebUtilsTest {

  @Test
  public void testWhenNoXForwardedForHeaderUseClientIp(){

    String clientIp = "127.0.0.1:10000";
    Map<String, String> headers = new HashMap<>();

    WebUtils utils = new WebUtils();

    String ip = utils.getRealClientIpAddr(headers, clientIp);

    assertEquals(ip, "127.0.0.1");
  }

  @Test
  public void testWhenClientIpNoPort(){

    String clientIp = "192.168.1.1";
    Map<String, String> headers = new HashMap<>();

    WebUtils utils = new WebUtils();

    String ip = utils.getRealClientIpAddr(headers, clientIp);

    assertEquals(ip, "192.168.1.1");
  }

  @Test
  public void testWhenXForwardedForHeaderUseHeader(){

    String clientIp = "127.0.0.1:10000";
    String upstreamIp = "192.168.1.1:10000";
    Map<String, String> headers = new HashMap<>();

    headers.put("X-Forwarded-For", upstreamIp);

    WebUtils utils = new WebUtils();

    String ip = utils.getRealClientIpAddr(headers, clientIp);

    assertEquals(ip, "192.168.1.1");
  }

  @Test
  public void testWhenXForwardedForHeaderMultipleUpstreamsUseHeader(){

    String clientIp = "127.0.0.1:10000";
    String upstreamIp = "192.168.1.1:10000";
    Map<String, String> headers = new HashMap<>();

    headers.put("X-Forwarded-For", upstreamIp + ",127.0.0.1,55.55.55.55");

    WebUtils utils = new WebUtils();

    String ip = utils.getRealClientIpAddr(headers, clientIp);

    assertEquals(ip, "192.168.1.1");
  }

}
