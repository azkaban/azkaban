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

import static azkaban.Constants.ConfigurationKeys.PROJECT_TEMP_DIR;
import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.storage.StorageManager;
import azkaban.user.User;
import azkaban.utils.Props;
import java.io.File;
import java.net.URL;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AzkabanProjectLoaderTest {

  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private final int ID = 107;
  private final int VERSION = 10;
  private final Project project = new Project(this.ID, "project1");

  private AzkabanProjectLoader azkabanProjectLoader;
  private StorageManager storageManager;
  private ProjectLoader projectLoader;

  @Before
  public void setUp() throws Exception {
    final Props props = new Props();
    props.put(PROJECT_TEMP_DIR, this.TEMP_DIR.getRoot().getAbsolutePath());

    this.storageManager = mock(StorageManager.class);
    this.projectLoader = mock(ProjectLoader.class);

    this.azkabanProjectLoader = new AzkabanProjectLoader(props, this.projectLoader,
        this.storageManager);
  }

  @Test
  public void uploadProject() throws Exception {
    when(this.projectLoader.getLatestProjectVersion(this.project)).thenReturn(this.VERSION);

    final URL resource = requireNonNull(
        getClass().getClassLoader().getResource("sample_flow_01.zip"));
    final File projectZipFile = new File(resource.getPath());
    final User uploader = new User("test_user");

    this.azkabanProjectLoader.uploadProject(this.project, projectZipFile, "zip", uploader, null);

    verify(this.storageManager)
        .uploadProject(this.project, this.VERSION + 1, projectZipFile, uploader);
  }

  @Test
  public void getProjectFile() throws Exception {
    when(this.projectLoader.getLatestProjectVersion(this.project)).thenReturn(this.VERSION);

    // Run the code
    this.azkabanProjectLoader.getProjectFile(this.project, -1);

    verify(this.projectLoader).getLatestProjectVersion(this.project);
    verify(this.storageManager).getProjectFile(this.ID, this.VERSION);
  }

}
