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

package azkaban.execapp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import azkaban.Constants.ConfigurationKeys;
import azkaban.executor.ExecutorLoader;
import azkaban.project.ProjectLoader;
import azkaban.spi.AzkabanEventReporter;
import azkaban.storage.StorageManager;
import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;

public class FlowRunnerManagerTest {

  private FlowRunnerManager instance;
  private ExecutorLoader executorLoader;
  private ProjectLoader projectLoader;
  private StorageManager storageManager;
  private TriggerManager triggerManager;
  private AzkabanEventReporter azkabanEventReporter;
  private Props props;


  @Before
  public void setup() throws IOException {
    this.props = new Props();
    this.props
        .put(ConfigurationKeys.PROJECT_DIR,
            Paths.get("src", "test", "resources", "projects").toString());
    this.executorLoader = mock(ExecutorLoader.class);
    this.projectLoader = mock(ProjectLoader.class);
    this.storageManager = mock(StorageManager.class);
    this.triggerManager = mock(TriggerManager.class);
    this.azkabanEventReporter = mock(AzkabanEventReporter.class);
    this.instance = new FlowRunnerManager(this.props, this.executorLoader, this.projectLoader,
        this.storageManager, this.triggerManager, this.azkabanEventReporter);
  }


  @Test
  public void testLoadExistingProjects() {
    final List<ProjectVersion> expected = Lists.newArrayList();
    expected.add(new ProjectVersion(1, 1, new File("src/test/resources/projects/1.1")));
    expected.add(new ProjectVersion(1, 2, new File("src/test/resources/projects/1.2")));
    expected.add(new ProjectVersion(1, 3, new File("src/test/resources/projects/1.3")));
    final List<ProjectVersion> actual = this.instance.loadExistingProjects();
    assertThat(actual).hasSameElementsAs(expected);
  }
}
