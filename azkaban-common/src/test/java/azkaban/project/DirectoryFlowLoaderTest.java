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

import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
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
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("exectest1"));
    Assert.assertEquals(0, loader.getErrors().size());
    Assert.assertEquals(5, loader.getFlowMap().size());
    Assert.assertEquals(2, loader.getPropsList().size());
    Assert.assertEquals(14, loader.getJobPropsMap().size());
  }

  @Test
  public void testLoadEmbeddedFlow() {
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("embedded"));
    Assert.assertEquals(0, loader.getErrors().size());
    Assert.assertEquals(2, loader.getFlowMap().size());
    Assert.assertEquals(0, loader.getPropsList().size());
    Assert.assertEquals(9, loader.getJobPropsMap().size());
  }

  @Test
  public void testRecursiveLoadEmbeddedFlow() {
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props());

    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("embedded_bad"));
    for (final String error : loader.getErrors()) {
      System.out.println(error);
    }

    // Should be 3 errors: jobe->innerFlow, innerFlow->jobe, innerFlow
    Assert.assertEquals(3, loader.getErrors().size());
  }
}
