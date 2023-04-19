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

package azkaban.utils;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;

public class ExternalLinkUtils {

  private static final Logger logger = Logger.getLogger(ExternalLinkUtils.class);

  public enum ExternalLinkScope {
    FLOW(""), JOB("job.");
    private final String name;

    ExternalLinkScope(final String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }
  }

  public enum ExternalLinkParams {
    EXEC_ID("exec_id"),
    JOB_ID("job_id"),
    WEBSERVER_HOST("webserver_host"),
    URL("url");
    private final String name;

    ExternalLinkParams(final String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }
  }

  static String encodeToUTF8(final String url) {
    try {
      return URLEncoder.encode(url, StandardCharsets.UTF_8.toString()).replaceAll("\\+", "%20");
    } catch (final UnsupportedEncodingException e) {
      logger.error("Specified encoding is not supported", e);
    }
    return "";
  }

  /**
   * Check if the external link url is reachable by making a http request to it. If the
   * response has 200x or 300x status code, returns true, and the external link is enabled or
   * rendered clickable in the UI, otherwise returns false, and the external link is rendered
   * disabled.
   */
  private static boolean validExternalLink(final String externalLinkUrl, final int timeout) {
    final RequestConfig config = RequestConfig.custom()
        .setConnectTimeout(timeout)
        .setConnectionRequestTimeout(timeout)
        .setSocketTimeout(timeout).build();
    final CloseableHttpClient client = HttpClients.custom()
        .setDefaultRequestConfig(config)
        .setSSLSocketFactory(null).disableRedirectHandling().build();
    try {
      try {
        final HttpResponse httpResponse = client.execute(new HttpGet(externalLinkUrl));
        final int responseCode = httpResponse.getStatusLine().getStatusCode();
        if (responseCode >= HttpStatus.SC_BAD_REQUEST) {
          logger.warn("validExternalLink url " + externalLinkUrl + " not reachable, response code "
              + responseCode);
          return false;
        }
        logger.debug("validExternalLink url " + externalLinkUrl + " is reachable.");
        return true;
      } finally {
        client.close();
      }
    } catch (Exception e) {
      logger.error("validExternalLink url " + externalLinkUrl + " CONNECT ERROR " + e);
      return false;
    }
  }

  /**
   * Parse all Flow or Job level external links that may have been configured in the
   * {@value Constants#AZKABAN_PROPERTIES_FILE} file.
   */
  public static List<ExternalLink> parseExternalLinks(final Props azkProps, final ExternalLinkScope level) {
    final Map<String, Object> params = new HashMap<>();
    params.put("level", level.getName());
    final String extLinksProperty =
        StrSubstitutor.replace(ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPICS, params, "${", "}");
    final Set<String> topics = new HashSet<>(azkProps.getStringList(extLinksProperty));

    final List<ExternalLink> externalLinks = new ArrayList<>();
    for (final String topic : topics) {
      params.put("topic", topic);
      final String label = azkProps.getString(StrSubstitutor.replace(
          ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC_LABEL, params, "${", "}"), "");
      final String url = azkProps.getString(StrSubstitutor.replace(
          ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC_URL, params, "${", "}"), "");

      if (url.isEmpty() || label.isEmpty()) {
        continue;
      }
      externalLinks.add(new ExternalLink(topic, label, url, true));
    }
    return externalLinks;
  }

  static String buildExternalLinkUrl(final String urlTemplate,
      final HttpServletRequest req, final int executionId, final String jobId) {
    final Map<String, Object> params = new HashMap<>();
    params.put(ExternalLinkParams.URL.getName(), encodeToUTF8(
        String.join("?", req.getRequestURL(), req.getQueryString())));
    params.put(ExternalLinkParams.EXEC_ID.getName(), executionId);
    params.put(ExternalLinkParams.JOB_ID.getName(), jobId);
    params.put(ExternalLinkParams.WEBSERVER_HOST.getName(), req.getServerName());
    return StrSubstitutor.replace(urlTemplate, params, "${", "}");
  }

  public static List<ExternalLink> buildExternalLinksForRequest(
      final List<ExternalLink> extLinksTemplates, final int extLinksTimeoutMs,
      final HttpServletRequest req, final int executionId, final String jobId) {
    final List<ExternalLink> extLinks = new ArrayList<>();
    for(final ExternalLink link: extLinksTemplates) {
      final String url = buildExternalLinkUrl(link.getLinkUrl(), req, executionId, jobId);
      extLinks.add(new ExternalLink(link.getTopic(), link.getLabel(), url,
          validExternalLink(url, extLinksTimeoutMs)));
    }
    return extLinks;
  }
}
