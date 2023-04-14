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

import azkaban.Constants.ConfigurationKeys;
import azkaban.utils.ExternalLinkUtils.ExternalLinkScope;
import azkaban.utils.ExternalLinkUtils.ExternalLinkParams;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.junit.Assert;
import org.junit.Test;

public class ExternalLinkUtilsTest {

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

  @Test
  public void testParseExternalLinks() {
    final Map<String, Object> params = new HashMap<>();
    params.put("level", ExternalLinkScope.FLOW.getName());
    params.put("topic", "topic1");
    final String flowLevelExternalLinkTopics =
        StrSubstitutor.replace(ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPICS, params,
            "${", "}");
    final String flowLevelExternalLinkTopic1Url =
        StrSubstitutor.replace(ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC_URL, params,
            "${", "}");
    final String flowLevelExternalLinkTopic1Label =
        StrSubstitutor.replace(ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC_LABEL, params,
            "${", "}");

    params.put("level", ExternalLinkScope.JOB.getName());
    params.put("topic", "jobTopic1");
    final String jobLevelExternalLinkTopics =
        StrSubstitutor.replace(ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPICS, params,
            "${", "}");
    final String jobLevelExternalLinkTopic1Url =
        StrSubstitutor.replace(ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC_URL, params,
            "${", "}");
    final String jobLevelExternalLinkTopic1Label =
        StrSubstitutor.replace(ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC_LABEL, params,
            "${", "}");

    params.put("topic", "jobMisconfiguredTopic");
    final String jobLevelExternalLinkMisconfiguredTopicUrl =
        StrSubstitutor.replace(ConfigurationKeys.AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC_URL, params,
            "${", "}");

    final ImmutableMap<String,String> confs = ImmutableMap.<String, String>builder()
        .put(flowLevelExternalLinkTopics, "topic1,notConfiguredTopic, topic1")
        .put(flowLevelExternalLinkTopic1Url,
            "http://url.com/${exec_id}/{$job_id}/${webserver_host}?url=${url}")
        .put(flowLevelExternalLinkTopic1Label, "Topic1Label")
        .put(jobLevelExternalLinkTopics, "jobTopic1, jobMisconfiguredTopic")
        .put(jobLevelExternalLinkTopic1Url,
            "http://joburl.com/${exec_id}/{$job_id}/${webserver_host}?url=${url}")
        .put(jobLevelExternalLinkTopic1Label, "JobTopic1Label")
        .put(jobLevelExternalLinkMisconfiguredTopicUrl, "http://joburl.com/u=${url}")
        .build();
    final Props azkabanProps = new Props(null, confs);

    // parse flow level external link configs
    final List<ExternalLink> expectedFlowLinks = ImmutableList.of(
        new ExternalLink("topic1", "Topic1Label",
            "http://url.com/${exec_id}/{$job_id}/${webserver_host}?url=${url}", true)
    );
    final List<ExternalLink> parsedLinks = ExternalLinkUtils.parseExternalLinks(azkabanProps, ExternalLinkScope.FLOW);

    Assert.assertEquals(expectedFlowLinks.size(), 1);
    Assert.assertEquals(expectedFlowLinks, parsedLinks);

    // parse job level external link configs
    final List<ExternalLink> expectedJobLinks = ImmutableList.of(
        new ExternalLink("jobTopic1", "JobTopic1Label",
            "http://joburl.com/${exec_id}/{$job_id}/${webserver_host}?url=${url}", true)
    );
    final List<ExternalLink> parsedJobLinks = ExternalLinkUtils.parseExternalLinks(azkabanProps,
        ExternalLinkScope.JOB);

    Assert.assertEquals(expectedJobLinks.size(), 1);
    Assert.assertEquals(expectedJobLinks, parsedJobLinks);
  }

  @Test
  public void testBuildExternalLinkUrl() {
    final HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    when(mockRequest.getRequestURL()).thenReturn(new StringBuffer("http://localhost:8081/executor"));
    when(mockRequest.getQueryString()).thenReturn("execid=1");
    when(mockRequest.getServerName()).thenReturn("localhost");

    String templateUrl1 = "http://url.com/params?p=";
    for(ExternalLinkParams p : ExternalLinkParams.values()) { // Enum.values() follow declaration order
      templateUrl1 += "|${" + p.getName() + "}";
    }
    final String expectedUrl1 = String.format("http://url.com/params?p=|12345|myJob|localhost|%s",
        ExternalLinkUtils.encodeToUTF8("http://localhost:8081/executor?execid=1"));
    String url = ExternalLinkUtils.buildExternalLinkUrl(templateUrl1, mockRequest, 12345,
        "myJob");
    Assert.assertEquals(expectedUrl1, url);

    final String templateUrl2 = "http://url.com/params?e=${exec_id}&u=${url}";
    final String expectedUrl2 = String.format("http://url.com/params?e=12345&u=%s",
        ExternalLinkUtils.encodeToUTF8("http://localhost:8081/executor?execid=1"));
    url = ExternalLinkUtils.buildExternalLinkUrl(templateUrl2, mockRequest, 12345,
        "myJob");
    Assert.assertEquals(expectedUrl2, url);

    // empty template url
    url = ExternalLinkUtils.buildExternalLinkUrl("", mockRequest, 12345,
        "myJob");
    Assert.assertEquals("", url);
  }
}
