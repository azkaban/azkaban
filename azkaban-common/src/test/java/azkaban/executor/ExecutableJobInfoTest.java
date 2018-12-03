/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban.executor;

import org.junit.Assert;
import org.junit.Test;

public class ExecutableJobInfoTest {

  private String parseImmediateFlowId(final String flowId) {
    // flowId pattern: flowRootName[,embeddedFlowName:embeddedFlowPath]*
    final ExecutableJobInfo jobInfo = new ExecutableJobInfo(1, 1, 1,
        flowId, "job", 0, 0, null, 0);

    return jobInfo.getImmediateFlowId();
  }

  @Test
  public void testParseFlowId() throws Exception {
    // flowId pattern: flowRootName[,embeddedFlowName:embeddedFlowPath]*
    Assert.assertEquals("Unexpected immediate flow id",
        "embedded", parseImmediateFlowId("embedded"));

    Assert.assertEquals("Unexpected immediate flow id",
        "embedded:emb_1", parseImmediateFlowId("embedded,emb_1:embedded:emb_1"));

    Assert.assertEquals("Unexpected immediate flow id",
        "embedded:emb_2:emb_3:emb_4",
        parseImmediateFlowId(
            "embedded,emb_2:embedded:emb_2,emb_3:embedded:emb_2:emb_3,emb_4:embedded:emb_2:emb_3:emb_4"));
  }
}
