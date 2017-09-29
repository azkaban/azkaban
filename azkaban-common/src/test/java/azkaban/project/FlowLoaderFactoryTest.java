/*
* Copyright 2017 LinkedIn Corp.
*
* Licensed under the Apache License, Version 2.0 (the “License”); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/

package azkaban.project;

import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import java.io.File;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class FlowLoaderFactoryTest {

  private static final String FLOW_10_TEST_DIRECTORY = "exectest1";
  private static final String FLOW_20_TEST_DIRECTORY = "basicflowyamltest";
  private Project project;

  @Before
  public void setUp() throws Exception {
    this.project = new Project(13, "myTestProject");
  }

  @Test
  public void testCreateDirectoryFlowLoader() {
    final FlowLoaderFactory loaderFactory = new FlowLoaderFactory(new Props(null));
    final File projectDir = ExecutionsTestUtil.getFlowDir(FLOW_10_TEST_DIRECTORY);
    final FlowLoader loader = loaderFactory.createFlowLoader(projectDir);
    Assert.assertTrue(loader instanceof DirectoryFlowLoader);
  }

  // Todo jamiesjc: add the manifest file with flow version 2.0 and enable this test
  @Test
  @Ignore
  public void testCreateDirectoryYamlFlowLoader() {
    final FlowLoaderFactory loaderFactory = new FlowLoaderFactory(new Props(null));
    final File projectDir = ExecutionsTestUtil.getFlowDir(FLOW_20_TEST_DIRECTORY);
    final FlowLoader loader = loaderFactory.createFlowLoader(projectDir);
    Assert.assertTrue(loader instanceof DirectoryYamlFlowLoader);
  }
}
