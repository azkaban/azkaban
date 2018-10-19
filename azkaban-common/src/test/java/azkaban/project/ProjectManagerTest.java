/*
* Copyright 2018 LinkedIn Corp.
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.executor.RunningExecutions;
import azkaban.storage.StorageManager;
import azkaban.user.User;
import azkaban.utils.Props;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for project manager
 */
public class ProjectManagerTest {

  private ProjectManager manager;
  private AzkabanProjectLoader azkabanProjectLoader;
  private ProjectLoader projectLoader;
  private StorageManager storageManager;
  private Props props;
  private RunningExecutions runningExecutions;

  @Before
  public void setUp() throws Exception {
    this.props = new Props();
    this.storageManager = mock(StorageManager.class);
    this.projectLoader = mock(ProjectLoader.class);
    this.runningExecutions = new RunningExecutions();
    this.azkabanProjectLoader = new AzkabanProjectLoader(this.props, this.projectLoader,
        this.storageManager, mock(FlowLoaderFactory.class), runningExecutions);
    this.manager = new ProjectManager(this.azkabanProjectLoader, this.projectLoader,
        this.storageManager, this.props);
  }

  @Test
  public void testCreateProjectsWithDifferentCases() {
    final String projectName = "mytestproject";
    final String projectDescription = "This is my new project with lower cases.";
    final User user = new User("testUser1");
    when(this.projectLoader.createNewProject(projectName, projectDescription, user))
        .thenReturn(new Project(1, projectName));
    this.manager.createProject(projectName, projectDescription, user);
    final String projectName2 = "MYTESTPROJECT";
    final String projectDescription2 = "This is my new project with UPPER CASES.";
    assertThatThrownBy(
        () -> this.manager.createProject(projectName2, projectDescription2, user))
        .isInstanceOf(ProjectManagerException.class)
        .hasMessageContaining(
            "Project already exists.");
  }
}
