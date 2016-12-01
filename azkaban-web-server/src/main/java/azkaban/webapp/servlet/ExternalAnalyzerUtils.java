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

package azkaban.webapp.servlet;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import azkaban.utils.Props;


public final class ExternalAnalyzerUtils {
  private static final Logger LOGGER = 
      Logger.getLogger(ExternalAnalyzerUtils.class.getName());
  public static final String EXECUTION_EXTERNAL_LINK_URL = 
      "execution.external.link.url";
  public static final String EXECUTION_EXTERNAL_LINK_LABEL = 
      "execution.external.link.label";

  private ExternalAnalyzerUtils() {

  }

  /**
   * Gets an external analyzer URL if configured in 'azkaban.properties'.
   * 
   * @param props    The props to be set to get the external analyzer URL. 
   *                 
   * @param req      The <code>HttpServletRequest</code> requesting the page.
   * 
   * @return         Returns an external analyzer URL.
   */
  public static String getExternalAnalyzer(Props props, HttpServletRequest req) {
    String url = props.getString(EXECUTION_EXTERNAL_LINK_URL, "");
    int index = url.indexOf('%');
    
    if (StringUtils.isNotEmpty(url) && index != -1) {
      String pattern = url.substring(url.indexOf('%'), url.length());

      switch (pattern) {        
        case "%url":
          return buildExternalAnalyzerURL(req, url, pattern);           
        default:
          LOGGER.error("Pattern configured is not supported. "
              + "Please check the comments section in 'azkaban.properties' "
              + "for supported patterns.");          
          return "";
      }      
    }
    LOGGER.debug("An optional external analyzer is not configured.");
    return "";
  }
  
  private static String 
  buildExternalAnalyzerURL(HttpServletRequest req, String url, String pattern) {
    StringBuilder builder = new StringBuilder();
    builder.append(req.getRequestURL());
    builder.append("?");
    builder.append(req.getQueryString());
    String flowExecutionURL = builder.toString();
    String encodedFlowExecUrl = "";
    try {
      encodedFlowExecUrl = URLEncoder.encode(flowExecutionURL, "UTF-8");
    } catch(UnsupportedEncodingException e) {
      LOGGER.error("Specified encoding is not supported", e);
    }
    return url.replaceFirst(pattern, encodedFlowExecUrl);
  }  
}
