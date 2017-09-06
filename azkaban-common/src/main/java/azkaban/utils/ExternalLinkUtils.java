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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import javax.servlet.http.HttpServletRequest;
import org.apache.log4j.Logger;

public class ExternalLinkUtils {

  private static final Logger logger = Logger.getLogger(ExternalLinkUtils.class);

  public static String getExternalAnalyzerOnReq(final Props azkProps,
      final HttpServletRequest req) {
    // If no topic was configured to be an external analyzer, return empty
    if (!azkProps.containsKey(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC)) {
      return "";
    }
    // Find out which external link we should use to lead to our analyzer
    final String topic = azkProps
        .getString(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC);
    return getLinkFromRequest(topic, azkProps, req);
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
    return azkProps.getString(
        Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_TOPIC_URL.replace("${topic}", topic),
        "");
  }

  static String encodeToUTF8(final String url) {
    try {
      return URLEncoder.encode(url, "UTF-8").replaceAll("\\+", "%20");
    } catch (final UnsupportedEncodingException e) {
      logger.error("Specified encoding is not supported", e);
    }
    return "";
  }
}
