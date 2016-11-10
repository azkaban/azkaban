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

import java.io.File;
import java.net.URL;
import java.net.URISyntaxException;

import azkaban.test.executions.TestExecutions;
import azkaban.utils.Props;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DirectoryFlowLoaderTest {
  private Project project;

  @Before
  public void setUp() {
    project = new Project(11, "myTestProject");
  }

  @Test
  public void testDirectoryLoad() throws URISyntaxException {
    Logger logger = Logger.getLogger(this.getClass());
    DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props(), logger);

    loader.loadProjectFlow(project, TestExecutions.getFlowDir("exectest1"));
    logger.info(loader.getFlowMap().size());
  }

  @Test
  public void testLoadEmbeddedFlow() throws URISyntaxException {
    Logger logger = Logger.getLogger(this.getClass());
    DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props(), logger);

    loader.loadProjectFlow(project, TestExecutions.getFlowDir("embedded"));
    Assert.assertEquals(0, loader.getErrors().size());
  }

  @Test
  public void testRecursiveLoadEmbeddedFlow() throws URISyntaxException {
    Logger logger = Logger.getLogger(this.getClass());
    DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props(), logger);

    loader.loadProjectFlow(project, TestExecutions.getFlowDir("embedded_bad"));
    for (String error : loader.getErrors()) {
      System.out.println(error);
    }

    // Should be 3 errors: jobe->innerFlow, innerFlow->jobe, innerFlow
    Assert.assertEquals(3, loader.getErrors().size());
  }
}
