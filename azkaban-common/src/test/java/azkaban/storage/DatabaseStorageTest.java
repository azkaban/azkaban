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

import azkaban.project.ProjectLoader;
import azkaban.spi.StorageMetadata;
import java.io.File;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class DatabaseStorageTest {
  private final ProjectLoader projectLoader = mock(ProjectLoader.class);
  private final DatabaseStorage databaseStorage = new DatabaseStorage(projectLoader);

  @Test
  public void testPut() throws Exception {
    final File file = mock(File.class);
    int projectId = 1234;
    int version = 1;
    String uploader = "testuser";
    final StorageMetadata metadata = new StorageMetadata(projectId, version, uploader, null);
    databaseStorage.put(metadata, file);
    verify(projectLoader).uploadProjectFile(projectId, version, file, uploader);
  }
}
