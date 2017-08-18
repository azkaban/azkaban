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

package azkaban.test.executions;


import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import org.junit.Test;

public class ExecutionsTestUtilTest {

  @Test
  public void getFlowFile() throws Exception {
    final String FLOW_NAME = "logtest";
    final String FILE_NAME = "largeLog1.log";
    final File testFile = ExecutionsTestUtil.getFlowFile(FLOW_NAME, FILE_NAME);
    assertThat(testFile).exists();
    assertThat(testFile).hasName(FILE_NAME);
  }

  @Test
  public void getDataRootDir() throws Exception {
    final String dataRootDir = ExecutionsTestUtil.getDataRootDir();
    assertThat(dataRootDir).isEqualTo("../test/execution-test-data/");
    final File rootDir = new File(dataRootDir);
    assertThat(rootDir).exists();
  }

  @Test
  public void getFlowDir() throws Exception {
    final String FLOW_NAME = "embedded";
    final File flowDir = ExecutionsTestUtil.getFlowDir(FLOW_NAME);
    assertThat(flowDir).exists();
    assertThat(flowDir).hasName(FLOW_NAME);
  }

}
