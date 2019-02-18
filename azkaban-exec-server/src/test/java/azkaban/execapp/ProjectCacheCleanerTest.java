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

package azkaban.execapp;

import java.io.File;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class ProjectCacheCleanerTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File projectsDir;

  @Before
  public void setUp() throws Exception {
    this.projectsDir = this.temporaryFolder.newFolder("projects");
  }

  @Test
  public void testProjectCacheDirCleaner() throws InterruptedException {
    /*
    final Long projectDirMaxSize = 3L;
    final ProjectCacheCleaner cleaner = null;

    //given
    final FlowPreparer flowPreparer = new FlowPreparer(createMockStorageManager(),
        this.executionsDir, this.projectsDir, projectDirMaxSize);

    final ExecutableFlow executableFlow = mock(ExecutableFlow.class);
    when(executableFlow.getExecutionId()).thenReturn(12345);
    when(executableFlow.getProjectId()).thenReturn(12);
    when(executableFlow.getVersion()).thenReturn(34);

    //when
    final List<File> expectedRemainingFiles = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      final int projectId = i;
      final int version = 1;
      final ProjectVersion pv = new ProjectVersion(projectId, version, null);
      flowPreparer.setup(pv);

      if (i >= 2) {
        //the first file will be deleted
        expectedRemainingFiles.add(pv.getInstalledDir());
      }
      // last modified time of millis second granularity of a file is not supported by all file
      // systems, so sleep for 1 second between creation of each project dir to make their last
      // modified time different.
      Thread.sleep(1000);
    }

    //then
    assertThat(this.projectsDir.listFiles()).containsExactlyInAnyOrder(expectedRemainingFiles
        .toArray(new File[expectedRemainingFiles.size()]));*/
  }

}
