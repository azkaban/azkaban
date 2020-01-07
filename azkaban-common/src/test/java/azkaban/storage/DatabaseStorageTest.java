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

package azkaban.storage;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import azkaban.db.DatabaseOperator;
import azkaban.project.ProjectLoader;
import azkaban.spi.ProjectStorageMetadata;
import azkaban.utils.Props;
import java.io.File;
import org.junit.Before;
import org.junit.Test;


public class DatabaseStorageTest {

  private final ProjectLoader projectLoader = mock(ProjectLoader.class);
  private final DatabaseStorage databaseStorage = new DatabaseStorage(this.projectLoader);

  @Test
  public void testPutProject() throws Exception {
    final File file = mock(File.class);
    final int projectId = 1234;
    final int version = 1;
    final String uploader = "testuser";
    final ProjectStorageMetadata metadata = new ProjectStorageMetadata(projectId, version, uploader, null);
    this.databaseStorage.putProject(metadata, file);
    verify(this.projectLoader).uploadProjectFile(projectId, version, file, uploader);
  }
}
