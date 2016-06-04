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


import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;

import org.junit.Before;
import org.junit.Test;

import azkaban.utils.Props;

public class ExternalAnalyzerUtilsTest {  
  private Props props;
  private HttpServletRequest mockRequest;
  private static final String EXEC_URL = "http://localhost:8081/executor";
  private static final String EXEC_QUERY_STRING = "execid=1";

  private static final String EXTERNAL_ANALYZER_URL_VALID_FORMAT = 
      "http://elephant.linkedin.com/search?q=%url";

  private static final String EXTERNAL_ANALYZER_URL_WRONG_FORMAT = 
      "http://elephant.linkedin.com/search?q=%unsupported";
  
  private static final String EXTERNAL_ANALYZER_URL_NO_FORMAT = 
      "http://elephant.linkedin.com/search?q=";
  
  private static final String EXTERNAL_ANALYZER_EXPECTED_URL = 
      "http://elephant.linkedin.com/search?q="
      + "http%3A%2F%2Flocalhost%3A8081%2Fexecutor%3Fexecid%3D1";
  
  @Before
  public void setUp() {
    props = new Props();
    mockRequest = mock(HttpServletRequest.class);         
  }
  
  /**
   * Test validates the happy path when an external analyzer is configured
   * with '%url' as the format in 'azkaban.properties'.
   */
  @Test
  public void testGetExternalAnalyzerValidFormat() {
    props.put(ExternalAnalyzerUtils.EXECUTION_EXTERNAL_LINK_URL, 
        EXTERNAL_ANALYZER_URL_VALID_FORMAT);

    when(mockRequest.getRequestURL()).thenReturn(new StringBuffer(EXEC_URL));
    when(mockRequest.getQueryString()).thenReturn(EXEC_QUERY_STRING);
    
    String executionExternalLinkURL = 
        ExternalAnalyzerUtils.getExternalAnalyzer(props, mockRequest);
    assertTrue(executionExternalLinkURL.equals(EXTERNAL_ANALYZER_EXPECTED_URL));
  }
  
  /**
   * Test validates the condition when an unsupported pattern is specified
   * in the url. e.g. '%url1', '%id' etc...
   */
  @Test
  public void testGetExternalAnalyzerWrongFormat() {
    props.put(ExternalAnalyzerUtils.EXECUTION_EXTERNAL_LINK_URL, 
        EXTERNAL_ANALYZER_URL_WRONG_FORMAT);
    
    String executionExternalLinkURL = 
        ExternalAnalyzerUtils.getExternalAnalyzer(props, mockRequest);
    assertTrue(executionExternalLinkURL.equals(""));
  }

  /**
   * Test validates the condition when '%url' is not specified.
   */
  @Test
  public void testGetExternalAnalyzerNoFormat() {
    props.put(ExternalAnalyzerUtils.EXECUTION_EXTERNAL_LINK_URL, 
        EXTERNAL_ANALYZER_URL_NO_FORMAT);
    
    String executionExternalLinkURL = 
        ExternalAnalyzerUtils.getExternalAnalyzer(props, mockRequest);
    assertTrue(executionExternalLinkURL.equals(""));
  }
  
  /**
   * Test validates the condition when an external analyzer is not configured
   * in 'azkaban.properties'.
   */
  @Test
  public void testGetExternalAnalyzerNotConfigured() {    
    String executionExternalLinkURL = 
        ExternalAnalyzerUtils.getExternalAnalyzer(props, mockRequest);
    assertTrue(executionExternalLinkURL.equals(""));
  }
}
