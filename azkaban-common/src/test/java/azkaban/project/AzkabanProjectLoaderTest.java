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
 *
 */

package azkaban.project;

import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.storage.StorageManager;
import azkaban.user.User;
import azkaban.utils.Props;
import java.io.File;
import java.net.URL;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AzkabanProjectLoaderTest {

  final int ID = 107;
  final int VERSION = 10;
  final Project project = new Project(ID, "project1");

  private AzkabanProjectLoader azkabanProjectLoader;
  private StorageManager storageManager;
  private ProjectLoader projectLoader;

  @Before
  public void setUp() throws Exception {
    tearDown();

    final Props props = new Props();
    storageManager = mock(StorageManager.class);
    projectLoader = mock(ProjectLoader.class);

    azkabanProjectLoader = new AzkabanProjectLoader(props, projectLoader, storageManager);
  }

  @After
  public void tearDown() throws Exception {
    if (azkabanProjectLoader != null) {
      FileUtils.deleteDirectory(azkabanProjectLoader.tempDir);
    }
  }

  @Test
  public void uploadProject() throws Exception {
    when(projectLoader.getLatestProjectVersion(project)).thenReturn(VERSION);

    final URL resource = requireNonNull(
        getClass().getClassLoader().getResource("sample_flow_01.zip"));
    final File projectZipFile = new File(resource.getPath());
    final User uploader = new User("test_user");

    azkabanProjectLoader.uploadProject(project, projectZipFile, "zip", uploader, null);

    verify(storageManager).uploadProject(project, VERSION + 1, projectZipFile, uploader);
  }

  @Test
  public void getProjectFile() throws Exception {
    when(projectLoader.getLatestProjectVersion(project)).thenReturn(VERSION);

    // Run the code
    azkabanProjectLoader.getProjectFile(project, -1);

    verify(projectLoader).getLatestProjectVersion(project);
    verify(storageManager).getProjectFile(ID, VERSION);
  }

}
