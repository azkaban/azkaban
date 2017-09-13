/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.project;

import azkaban.Constants;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DirectoryFlowLoaderTest {

  private Project project;

  @Before
  public void setUp() {
    this.project = new Project(11, "myTestProject");
  }

  @Test
  public void testDirectoryLoad() {
    final Logger logger = Logger.getLogger(this.getClass());
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props(), logger);

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("exectest1"), null);
    logger.info(loader.getFlowMap().size());
  }

  @Test
  public void testLoadEmbeddedFlow() {
    final Logger logger = Logger.getLogger(this.getClass());
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props(), logger);

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("embedded"), null);
    Assert.assertEquals(0, loader.getErrors().size());
  }

  @Test
  public void testRecursiveLoadEmbeddedFlow() {
    final Logger logger = Logger.getLogger(this.getClass());
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props(), logger);

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("embedded_bad"), null);
    for (final String error : loader.getErrors()) {
      System.out.println(error);
    }

    // Should be 3 errors: jobe->innerFlow, innerFlow->jobe, innerFlow
    Assert.assertEquals(3, loader.getErrors().size());
  }

  @Test
  public void testLoadBasicFlowYaml() {
    final Logger logger = Logger.getLogger(this.getClass());
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props(), logger);

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("basicflowyamltest"),
        Constants.AZKABAN_FLOW_VERSION_2_0);
    Assert.assertEquals(1, loader.getFlowMap().size());
    Assert.assertTrue(loader.getFlowMap().containsKey("basic_flow_shell_end"));
    Assert.assertEquals(4, loader.getJobProps().size());
    Assert.assertTrue(loader.getJobProps().containsKey("basic_flow_shell_end"));
    Assert.assertTrue(loader.getJobProps().containsKey("basic_flow_shell_pwd"));
    Assert.assertTrue(loader.getJobProps().containsKey("basic_flow_shell_echo"));
    Assert.assertTrue(loader.getJobProps().containsKey("basic_flow_shell_bash"));
  }

  //Todo jamiesjc: add tests for loading embedded flow yaml
}
