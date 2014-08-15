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

package azkaban.utils;

import java.io.File;

import org.apache.log4j.Logger;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class DirectoryFlowLoaderTest {

  @Ignore @Test
  public void testDirectoryLoad() {
    Logger logger = Logger.getLogger(this.getClass());
    DirectoryFlowLoader loader = new DirectoryFlowLoader(logger);

    loader.loadProjectFlow(new File("unit/executions/exectest1"));
    logger.info(loader.getFlowMap().size());
  }

  @Ignore @Test
  public void testLoadEmbeddedFlow() {
    Logger logger = Logger.getLogger(this.getClass());
    DirectoryFlowLoader loader = new DirectoryFlowLoader(logger);

    loader.loadProjectFlow(new File("unit/executions/embedded"));
    Assert.assertEquals(0, loader.getErrors().size());
  }

  @Ignore @Test
  public void testRecursiveLoadEmbeddedFlow() {
    Logger logger = Logger.getLogger(this.getClass());
    DirectoryFlowLoader loader = new DirectoryFlowLoader(logger);

    loader.loadProjectFlow(new File("unit/executions/embeddedBad"));
    for (String error : loader.getErrors()) {
      System.out.println(error);
    }

    // Should be 3 errors: jobe->innerFlow, innerFlow->jobe, innerFlow
    Assert.assertEquals(3, loader.getErrors().size());
  }
}
