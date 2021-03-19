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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.Constants;
import java.util.Arrays;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExternalLinkUtilsTest {

  private static final String EXEC_URL = "http://localhost:8081/executor";
  private static final String EXEC_QUERY_STRING = "execid=1";
  private static final String EXTERNAL_ANALYZER_TOPIC = "elephant";
  private static final String EXTERNAL_ANALYZER_TOPICS = "topic1,topic2";
  private static final String EXTERNAL_ANALYZER_TOPIC1 = "topic1";
  private static final String EXTERNAL_ANALYZER_TOPIC2 = "topic2";

  private static final String EXTERNAL_ANALYZER_LABEL1 = "topic1";
  private static final String EXTERNAL_ANALYZER_LABEL2 = "topic2";

  private static final String EXTERNAL_ANALYZER_URL_VALID_FORMAT =
      "http://elephant.linkedin.com/search?q=${url}";
  private static final String EXTERNAL_ANALYZER_EXPECTED_URL =
      "http://elephant.linkedin.com/search?q="
          + "http%3A%2F%2Flocalhost%3A8081%2Fexecutor%3Fexecid%3D1";

  private static final String EXTERNAL_ANALYZER_URL_VALID_FORMAT2 =
      "http://elephant2.linkedin.com/search?q=${url}";
  private static final String EXTERNAL_ANALYZER_EXPECTED_URL2 =
      "http://elephant2.linkedin.com/search?q="
          + "http%3A%2F%2Flocalhost%3A8081%2Fexecutor%3Fexecid%3D1";

  private static final String EXTERNAL_LOGVIEWER_TOPIC = "kibana";
  private static final String EXTERNAL_LOGVIEWER_URL_VALID_FORMAT =
      "http://kibana.linkedin.com/search?jobid=${jobid}&&execid=${execid}";
  private static final String EXTERNAL_LOGVIEWER_EXPECTED_URL =
      "http://kibana.linkedin.com/search?jobid=Some%20%2B%20job&&execid=1";
  private Props azkProps;
  private Props jobProps;
  private String jobId;
  private HttpServletRequest mockRequest;

  @Before
  public void setUp() {
    // Empty server configuration
    this.azkProps = new Props();

    // Job configuration consisting of only an exec id and job id
    this.jobProps = new Props();
    this.jobProps.put(Constants.FlowProperties.AZKABAN_FLOW_EXEC_ID, 1);
    this.jobId = "Some + job";

    this.mockRequest = mock(HttpServletRequest.class);
  }

  /**
   * Test validates the happy path when an log viewer is configured with '${execid}'  and '${jobid}
   * as the format in 'azkaban.properties'.
   */
  @Test
  public void testGetExternalLogViewerValidFormat() {
    this.azkProps.put(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_LOGVIEWER_TOPIC,
        EXTERNAL_LOGVIEWER_TOPIC);
    this.azkProps.put(
        Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_TOPIC_URL
            .replace("${topic}", EXTERNAL_LOGVIEWER_TOPIC),
        EXTERNAL_LOGVIEWER_URL_VALID_FORMAT);

    final String externalURL =
        ExternalLinkUtils.getExternalLogViewer(this.azkProps, this.jobId, this.jobProps);
    assertTrue(externalURL.equals(EXTERNAL_LOGVIEWER_EXPECTED_URL));
  }

  /**
   * Test validates the condition when an external log viewer is not configured in
   * 'azkaban.properties'.
   */
  @Test
  public void testGetLogViewerNotConfigured() {
    final String executionExternalLinkURL =
        ExternalLinkUtils.getExternalLogViewer(this.azkProps, this.jobId, this.jobProps);
    assertTrue(executionExternalLinkURL.equals(""));
  }

  /**
   * Test validates that when we encode URLs to UTF-8, it does not give us incorrect encodings.
   */
  @Test
  public void testEncodingToUFT8() {
    assertTrue(ExternalLinkUtils.encodeToUTF8(" ").equals("%20"));
    assertTrue(ExternalLinkUtils.encodeToUTF8("+").equals("%2B"));
    assertTrue(ExternalLinkUtils.encodeToUTF8("/").equals("%2F"));
    assertTrue(ExternalLinkUtils.encodeToUTF8(":").equals("%3A"));
    assertTrue(ExternalLinkUtils.encodeToUTF8("?").equals("%3F"));
    assertTrue(ExternalLinkUtils.encodeToUTF8("=").equals("%3D"));
  }

  /**
   * Make sure that URLs for analyzers and logviewers are fetched correctly by setting it manually
   * and then fetching them
   */
  @Test
  public void testFetchURL() {
    this.azkProps.put(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_TOPIC_URL
        .replace("${topic}", "someTopic"), "This is a link");
    assertTrue(
        ExternalLinkUtils.getURLForTopic("someTopic", this.azkProps).equals("This is a link"));
  }
  /**
   * Test validates the GetExternalAnalyzerTopics
   * format in 'azkaban.properties'. it should work with both
   * azkaban.server.external.analyzer.topic and azkaban.server.external.analyzer.topics
   */
  @Test
  public void testGetExternalAnalyzerTopics() {
    List<String> topicsExpected = Arrays.asList(EXTERNAL_ANALYZER_TOPIC1);
    this.azkProps.put(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC,
        EXTERNAL_ANALYZER_TOPIC1);

    List<String> topicsReturned = ExternalLinkUtils.getExternalAnalyzerTopics(this.azkProps);
    Assert.assertArrayEquals(topicsExpected.toArray(), topicsReturned.toArray());

    this.azkProps.put(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPICS,
        EXTERNAL_ANALYZER_TOPIC1);
    topicsReturned = ExternalLinkUtils.getExternalAnalyzerTopics(this.azkProps);
    Assert.assertArrayEquals(topicsExpected.toArray(), topicsReturned.toArray());


    topicsExpected = Arrays.asList(EXTERNAL_ANALYZER_TOPIC1, EXTERNAL_ANALYZER_TOPIC2);
    this.azkProps.put(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPICS,
        EXTERNAL_ANALYZER_TOPICS);
    topicsReturned = ExternalLinkUtils.getExternalAnalyzerTopics(this.azkProps);
    Assert.assertArrayEquals(topicsExpected.toArray(), topicsReturned.toArray());
  }
  /**
   * Test validates the getExternalLabelForTopic
   * format in 'azkaban.properties'. it should work with both
   * azkaban.server.external.analyzer.topic and azkaban.server.external.analyzer.topics
   */
  @Test
  public void testGetExternalLabelForTopic() {
    String topicLabelExpected = EXTERNAL_ANALYZER_LABEL1;
    this.azkProps.put(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_LABEL,
        EXTERNAL_ANALYZER_LABEL1);
    String topicLabelReturned =
        ExternalLinkUtils.getExternalLabelForTopic(EXTERNAL_ANALYZER_TOPIC, this.azkProps);
    Assert.assertEquals(topicLabelExpected, topicLabelReturned);


    this.azkProps.put(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC_LABEL
        .replace("${topic}", EXTERNAL_ANALYZER_TOPIC1), EXTERNAL_ANALYZER_LABEL1);
    topicLabelReturned =
        ExternalLinkUtils.getExternalLabelForTopic(EXTERNAL_ANALYZER_TOPIC1, this.azkProps);
    Assert.assertEquals(topicLabelExpected, topicLabelReturned);

    topicLabelExpected = EXTERNAL_ANALYZER_LABEL2;
    this.azkProps.put(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC_LABEL
        .replace("${topic}", EXTERNAL_ANALYZER_TOPIC2), EXTERNAL_ANALYZER_LABEL2);
    topicLabelReturned =
        ExternalLinkUtils.getExternalLabelForTopic(EXTERNAL_ANALYZER_TOPIC2, this.azkProps);
    Assert.assertEquals(topicLabelExpected, topicLabelReturned);
  }

  /**
   * Test validates the happy path when an external analyzer is configured with '${url}' as the
   * format in 'azkaban.properties'.
   */
  @Test
  public void testGetExternalAnalyzerValidFormat() {
    this.azkProps.put(Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC,
        EXTERNAL_ANALYZER_TOPIC);
    this.azkProps.put(
        Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_TOPIC_URL
            .replace("${topic}", EXTERNAL_ANALYZER_TOPIC1),
        EXTERNAL_ANALYZER_URL_VALID_FORMAT);

    when(this.mockRequest.getRequestURL()).thenReturn(new StringBuffer(EXEC_URL));
    when(this.mockRequest.getQueryString()).thenReturn(EXEC_QUERY_STRING);

    String externalURL =
        ExternalLinkUtils.getExternalAnalyzerLinkOnReq(EXTERNAL_ANALYZER_TOPIC1, this.azkProps,
            this.mockRequest);
    assertTrue(externalURL.equals(EXTERNAL_ANALYZER_EXPECTED_URL));

    this.azkProps.put(
        Constants.ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC_URL
            .replace("${topic}", EXTERNAL_ANALYZER_TOPIC1),
        EXTERNAL_ANALYZER_URL_VALID_FORMAT2);

    externalURL =
        ExternalLinkUtils.getExternalAnalyzerLinkOnReq(EXTERNAL_ANALYZER_TOPIC1, this.azkProps,
            this.mockRequest);
    assertTrue(externalURL.equals(EXTERNAL_ANALYZER_EXPECTED_URL2));
  }
  /**
   * Test validates the condition when an external analyzer is not configured in
   * 'azkaban.properties'.
   */
  @Test
  public void testGetExternalAnalyzerNotConfigured() {
    final String executionExternalLinkURL =
        ExternalLinkUtils.getExternalAnalyzerLinkOnReq(EXTERNAL_ANALYZER_TOPIC1, this.azkProps,
            this.mockRequest);
    assertTrue(executionExternalLinkURL.equals(""));
  }
}
