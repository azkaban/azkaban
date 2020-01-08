/*
 * Copyright 2020 LinkedIn Corp.
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
 *
 */

package azkaban.webapp;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertNotNull;

import azkaban.flowtrigger.quartz.FlowTriggerScheduler;
import azkaban.project.DirectoryFlowLoader;
import azkaban.project.Project;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Test;


public class FlowTriggerSchedulerTest extends AzkabanWebServerTest {

  @Test
  @Override
  public void test() throws Exception {
    final Injector injector = getInjector(60001);

    Project project = new Project(1, "myTestProject");
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(new Props());
    loader.loadProjectFlow(project, ExecutionsTestUtil.getFlowDir("yamlcrontest"));
    Assert.assertEquals(0, loader.getErrors().size());
    project.setFlows(loader.getFlowMap());
    project.setVersion(123);

    injector.getInstance(FlowTriggerScheduler.class).schedule(project, "admin");

    SERVICE_PROVIDER.unsetInjector();
  }

}
