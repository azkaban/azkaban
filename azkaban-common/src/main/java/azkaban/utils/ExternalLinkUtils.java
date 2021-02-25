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
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;
import org.apache.http.HttpStatus;

public class ExternalLinkUtils {
  private static final int DEFAULT_AZKABAN_SERVER_EXTERNAL_TIMEOUT_MS = 3000; //3 seconds

  private static final Logger logger = Logger.getLogger(ExternalLinkUtils.class);

  public static String getExternalAnalyzerLinkOnReq(final String topic, final Props azkProps,
      final HttpServletRequest req) {
    // If no topic was configured to be an external analyzer, return empty
    return topic.isEmpty() ? "" : getLinkFromRequest(topic, azkProps, req);
  }

  static List<String> getExternalAnalyzerTopics(final Props azkProps) {
    if (azkProps.containsKey(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPICS)) {
      return azkProps.getStringList(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPICS);
    }
    // If no AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPICS defined, use legacy config
    //AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC
    return Arrays.asList(azkProps.getString(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC, ""));
  }

  public static String getExternalLogViewer(final Props azkProps, final String jobId,
      final Props jobProps) {
    // If no topic was configured to be an external analyzer, return empty
    if (!azkProps
        .containsKey(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_LOGVIEWER_TOPIC)) {
      return "";
    }
    // Find out which external link we should use to lead to our log viewer
    final String topic = azkProps
        .getString(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_LOGVIEWER_TOPIC);
    return getLinkFromJobAndExecId(topic, azkProps, jobId, jobProps);
  }

  private static String getLinkFromJobAndExecId(final String topic, final Props azkProps,
      final String jobId,
      final Props jobProps) {
    String urlTemplate = getURLForTopic(topic, azkProps);
    if (urlTemplate.isEmpty()) {
      logger.error("No URL specified for topic " + topic);
      return "";
    }
    final String job = encodeToUTF8(jobId);
    final String execid = encodeToUTF8(
        jobProps.getString(Constants.FlowProperties.AZKABAN_FLOW_EXEC_ID));

    urlTemplate = urlTemplate.replace("${jobid}", job).replace("${execid}", execid);
    logger.info("Creating link: " + urlTemplate);
    return urlTemplate;
  }

  private static String getLinkFromRequest(final String topic, final Props azkProps,
      final HttpServletRequest req) {
    String urlTemplate = getURLForTopic(topic, azkProps);
    if (urlTemplate.isEmpty()) {
      logger.error("No URL specified for topic " + topic);
      return "";
    }
    String flowExecutionURL = "";
    flowExecutionURL += req.getRequestURL();
    flowExecutionURL += "?";
    flowExecutionURL += req.getQueryString();
    flowExecutionURL = encodeToUTF8(flowExecutionURL);
    urlTemplate = urlTemplate.replace("${url}", flowExecutionURL);
    logger.info("Creating link: " + urlTemplate);
    return urlTemplate;
  }

  static String getURLForTopic(final String topic, final Props azkProps) {
    String externalUrlConfKeyToUse =
        Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC_URL.replace("${topic"
            + "}", topic);
    // If no AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC_URL defined, use legacy config
    //AZKABAN_SERVER_EXTERNAL_TOPIC_URL
    externalUrlConfKeyToUse = azkProps.containsKey(externalUrlConfKeyToUse) ?
        externalUrlConfKeyToUse :
        Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_TOPIC_URL.replace("$"
            + "{topic}", topic);
    return azkProps.getString(externalUrlConfKeyToUse, "");
  }

  static String getExternalLabelForTopic(final String topic, final Props azkProps) {
    String externalLabelConfKeyToUse =
        Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC_LABEL.replace("${topic"
            + "}", topic);
    // If no AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC_LABEL defined, use legacy config
    //AZKABAN_SERVER_EXTERNAL_ANALYZER_LABEL
    externalLabelConfKeyToUse = azkProps.containsKey(externalLabelConfKeyToUse) ?
        externalLabelConfKeyToUse :
        Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_LABEL;
    return azkProps.getString(externalLabelConfKeyToUse, "External Analyzer");
  }

  static String encodeToUTF8(final String url) {
    try {
      return URLEncoder.encode(url, "UTF-8").replaceAll("\\+", "%20");
    } catch (final UnsupportedEncodingException e) {
      logger.error("Specified encoding is not supported", e);
    }
    return "";
  }

  /**
   * Return list of external analyzers based on configs and current web request to web server
   * to render the execution page.
   * @param azkProps azkaban configs
   * @param req current web request
   * @return list of external analyzers
   */
  public static List<ExternalLink> getExternalAnalyzers( final Props azkProps, final HttpServletRequest req) {
    final List<String> externalTopics = getExternalAnalyzerTopics(azkProps);
    return externalTopics.stream().map(topic -> {
      String externalLinkUrl = getExternalAnalyzerLinkOnReq(topic, azkProps, req);
      if (!externalLinkUrl.isEmpty()) {
        logger.debug("Adding an External analyzer to the page");
        final String execExternalLinkLabel = getExternalLabelForTopic(topic, azkProps);
        logger.debug("External analyzer label set to : " + execExternalLinkLabel);
        int timeout =
            azkProps.getInt(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TIMEOUT_MS,
            DEFAULT_AZKABAN_SERVER_EXTERNAL_TIMEOUT_MS);
        return new ExternalLink(topic, execExternalLinkLabel, externalLinkUrl,
            validExternalLink(externalLinkUrl, timeout));
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());
  }

  /**
   * check if external link is reachable or not.
   * It will send http request to target url, and check the status code from response,
   * If the page returns 200x or 300x status, returns true, and external link is enabled,
   * otherwise returns false, and external link is disabled.
   * @param externalLinkUrl azkaban configs
   * @param timeout timeout for httpClient in milliseconds
   * @return true if target link is reachable, otherwise false.
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
      HttpResponse httpResponse = client.execute(new HttpGet(externalLinkUrl));
      int responseCode = httpResponse.getStatusLine().getStatusCode();
      if (responseCode >= HttpStatus.SC_BAD_REQUEST) {
        logger.warn("validExternalLink url " + externalLinkUrl + " not reachable, response code " + responseCode);
        return false;
      }
      logger.debug("validExternalLink url " + externalLinkUrl + " is reachable.");
      client.close();
      return true;
    } catch (IOException e) {
      logger.error("validExternalLink url " + externalLinkUrl + " CONNECT IO ERROR " + e);
      return false;
    } catch (Exception e) {
      logger.error("validExternalLink url " + externalLinkUrl + " CONNECT ERROR " + e);
      return false;
    }
  }
}
