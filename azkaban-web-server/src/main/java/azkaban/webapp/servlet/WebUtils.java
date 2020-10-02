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

package azkaban.webapp.servlet;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_HOST_NAME;

import azkaban.ServiceProvider;
import azkaban.executor.Status;
import azkaban.spi.AzkabanEventReporter;
import azkaban.spi.EventType;
import azkaban.webapp.AzkabanWebServer;
import com.google.common.base.Strings;
import com.google.inject.ConfigurationException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.apache.log4j.Logger;

public class WebUtils {

  public static final String X_FORWARDED_FOR_HEADER = "X-Forwarded-For";
  private static final long ONE_KB = 1024;
  private static final long ONE_MB = 1024 * ONE_KB;
  private static final long ONE_GB = 1024 * ONE_MB;
  private static final long ONE_TB = 1024 * ONE_GB;

  private static AzkabanEventReporter azkabanEventReporter;

  static {
    try {
      azkabanEventReporter = ServiceProvider.SERVICE_PROVIDER
          .getInstance(AzkabanEventReporter.class);
    } catch (Exception e) {
      Logger.getLogger(WebUtils.class.getName()).warn("AzkabanEventReporter not configured", e);
    }
  }

  public static String displayBytes(final long sizeBytes) {
    final NumberFormat nf = NumberFormat.getInstance();
    nf.setMaximumFractionDigits(2);
    if (sizeBytes >= ONE_TB) {
      return nf.format(sizeBytes / (double) ONE_TB) + " tb";
    } else if (sizeBytes >= ONE_GB) {
      return nf.format(sizeBytes / (double) ONE_GB) + " gb";
    } else if (sizeBytes >= ONE_MB) {
      return nf.format(sizeBytes / (double) ONE_MB) + " mb";
    } else if (sizeBytes >= ONE_KB) {
      return nf.format(sizeBytes / (double) ONE_KB) + " kb";
    } else {
      return sizeBytes + " B";
    }
  }

  public static String formatStatus(final Status status) {
    switch (status) {
      case SUCCEEDED:
        return "Success";
      case FAILED:
        return "Failed";
      case RUNNING:
        return "Running";
      case DISABLED:
        return "Disabled";
      case KILLED:
        return "Killed";
      case FAILED_FINISHING:
        return "Running w/Failure";
      case PREPARING:
        return "Preparing";
      case READY:
        return "Ready";
      case PAUSED:
        return "Paused";
      case SKIPPED:
        return "Skipped";
      case KILLING:
        return "Killing";
      default:
    }
    return "Unknown";
  }

  /**
   * Gets the actual client IP address on a best effort basis as user could be sitting
   * behind a VPN. Get the IP by inspecting the X-Forwarded-For HTTP header or using the
   * provided 'remote IP address' from the low level TCP connection from the client.
   *
   * If multiple IP addresses are provided in the X-Forwarded-For header then the first one (first
   * hop) is used
   *
   * @param httpHeaders List of HTTP headers for the current request
   * @param remoteAddr The client IP address and port from the current request's TCP connection
   * @return The actual client IP address
   */
  // TODO djaiswal83: Refactor this code and merge into single API
  public static String getRealClientIpAddr(final Map<String, String> httpHeaders,
      final String remoteAddr) {

    // If some upstream device added an X-Forwarded-For header
    // use it for the client ip
    // This will support scenarios where load balancers or gateways
    // front the Azkaban web server and a changing Ip address invalidates the session

    String clientIp = httpHeaders.getOrDefault(X_FORWARDED_FOR_HEADER, null);
    if (clientIp == null) {
      clientIp = remoteAddr;
    } else {
      // header can contain comma separated list of upstream servers - get the first one
      final String[] ips = clientIp.split(",");
      clientIp = ips[0];
    }

    // Strip off port and only get IP address
    // todo: this is broken for IPv6, where e.g. a "loopback" address looks like "0:0:0:0:0:0:0:1"
    final String[] parts = clientIp.split(":");
    clientIp = parts[0];

    return clientIp;
  }

  /**
   * Gets the actual client IP address on a best effort basis as user could be sitting
   * behind a VPN. Get the IP by inspecting the X-Forwarded-For HTTP header or using the
   * provided 'remote IP address' from the low level TCP connection from the client.
   *
   * If multiple IP addresses are provided in the X-Forwarded-For header then the first one (first
   * hop) is used
   *
   * @param req HttpServletRequest
   * @return The actual client IP address
   */
  public static String getRealClientIpAddr(final HttpServletRequest req) {

    // If some upstream device added an X-Forwarded-For header
    // use it for the client ip
    // This will support scenarios where load balancers or gateways
    // front the Azkaban web server and a changing Ip address invalidates
    // the session
    final HashMap<String, String> headers = new HashMap<>();
    headers.put(WebUtils.X_FORWARDED_FOR_HEADER,
        req.getHeader(WebUtils.X_FORWARDED_FOR_HEADER.toLowerCase()));

    return WebUtils.getRealClientIpAddr(headers, req.getRemoteAddr());
  }

  private static String hostName;

  static {
    try {
      hostName = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      hostName = "unknown";
    }
  }

  /**
   * Report login/logout events via {@link AzkabanEventReporter}, if configured.
   * @param eventType login or logout
   * @param username if known
   * @param ip address of originating host
   * @param isSuccess AKA outcome
   * @param message AKA reason
   */
  public static void reportLoginEvent(final EventType eventType, final String username,
      final String ip, final boolean isSuccess, final String message) {

    if (azkabanEventReporter != null) {
      final Map<String, String> metadata = new HashMap<>();
      metadata.put("azkabanHost",
          AzkabanWebServer.getAzkabanProperties().getString(AZKABAN_SERVER_HOST_NAME, hostName));
      metadata.put("sessionUser", Strings.isNullOrEmpty(username) ? "unknown" : username);
      metadata.put("sessionIP", ip);
      metadata.put("reason", message);
      metadata.put("appOutcome", isSuccess ? "SUCCESS" : "FAILURE");

      azkabanEventReporter.report(eventType, metadata);
    }
  }

  public static void reportLoginEvent(final EventType eventType, final String username, final String ip) {
    reportLoginEvent(eventType, username, ip, true, null);
  }
}
