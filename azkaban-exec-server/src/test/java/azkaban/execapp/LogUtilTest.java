/*
 * Copyright 2017 LinkedIn Corp.
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

package azkaban.execapp;


import static org.assertj.core.api.Assertions.assertThat;

import azkaban.utils.Props;
import org.junit.Test;

public class LogUtilTest {

  @Test
  public void createLogPatternLayoutJsonObject() throws Exception {
    final String jobId = "jobId1";
    final Props props = Props.of("azkaban.flow.projectname", "projectFoo",
        "azkaban.flow.flowid", "flowId1",
        "azkaban.flow.submituser", "submitUserFoo",
        "azkaban.flow.execid", "execId1",
        "azkaban.flow.projectversion", "projectV1");
    final String expected = "{\"jobid\":\"jobId1\","
        + "\"projectname\":\"projectFoo\","
        + "\"level\":\"%p\","
        + "\"submituser\":\"submitUserFoo\","
        + "\"projectversion\":\"projectV1\","
        + "\"category\":\"%c{1}\","
        + "\"message\":\"%m\","
        + "\"logsource\":\"userJob\","
        + "\"flowid\":\"flowId1\","
        + "\"execid\":\"execId1\"}";
    final String result = LogUtil.createLogPatternLayoutJsonString(props, jobId);
    assertThat(result).isEqualToIgnoringWhitespace(expected);
  }

}
