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

package azkaban.webapp.servlet;

import static org.junit.Assert.assertEquals;

import azkaban.spi.EventType;
import java.util.HashMap;
import java.util.Map;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test class for azkaban.webapp.servlet.WebUtils
 */
public class WebUtilsTest {

  @Test
  public void testWhenNoXForwardedForHeaderUseClientIp() {

    final String clientIp = "127.0.0.1:10000";
    final Map<String, String> headers = new HashMap<>();
    final String ip = WebUtils.getRealClientIpAddr(headers, clientIp);

    assertEquals(ip, "127.0.0.1");
  }

  @Test
  public void testWhenClientIpNoPort() {

    final String clientIp = "192.168.1.1";
    final Map<String, String> headers = new HashMap<>();
    final String ip = WebUtils.getRealClientIpAddr(headers, clientIp);

    assertEquals(ip, "192.168.1.1");
  }

  @Test
  public void testWhenXForwardedForHeaderUseHeader() {

    final String clientIp = "127.0.0.1:10000";
    final String upstreamIp = "192.168.1.1:10000";
    final Map<String, String> headers = new HashMap<>();

    headers.put("X-Forwarded-For", upstreamIp);

    final String ip = WebUtils.getRealClientIpAddr(headers, clientIp);

    assertEquals(ip, "192.168.1.1");
  }

  @Test
  public void testWhenXForwardedForHeaderMultipleUpstreamsUseHeader() {

    final String clientIp = "127.0.0.1:10000";
    final String upstreamIp = "192.168.1.1:10000";
    final Map<String, String> headers = new HashMap<>();

    headers.put("X-Forwarded-For", upstreamIp + ",127.0.0.1,55.55.55.55");

    final String ip = WebUtils.getRealClientIpAddr(headers, clientIp);

    assertEquals(ip, "192.168.1.1");
  }

  @Test
  @Ignore  // will fail right now
  public void testIPv6NoPort() {

    final String clientIp = "fe80::aede:48ff:.55805";
    // or ::1, or fe80::1cc4:170d:463d:99d1%en0, or http://[fe80::1ff:fe23:4567:890a%25eth0]:443/
    final Map<String, String> headers = new HashMap<>();
    final String ip = WebUtils.getRealClientIpAddr(headers, clientIp);

    assertEquals(ip, "fe80::aede:48ff");
  }

  @Test
  public void testReportLoginEvent() {
    // can't really test much without a way to inject a mock of AzkabanEventReporter into
    // WebUtils, but let's at least invoke it anyway:
    WebUtils.reportLoginEvent(EventType.USER_LOGIN, "itsme", "127.0.0.1");
  }
}
