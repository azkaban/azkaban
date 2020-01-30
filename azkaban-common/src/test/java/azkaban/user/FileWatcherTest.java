/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.user;

import static org.junit.Assert.assertTrue;

import com.google.common.io.Resources;
import com.sun.nio.file.SensitivityWatchEventModifier;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileWatcherTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Path PATH;
  private FileWatcher fileWatcher;

  private Path setPath() throws IOException {
    final URL configURL = Resources.getResource("test-conf/azkaban-users-test1.xml");
    final String origPathStr = configURL.getPath();

    // Create a new directory and copy the file in it.
    final Path workDir = temporaryFolder.newFolder().toPath();
    // Copy the file to keep original file unmodified
    final String path = workDir.toString() + "/azkaban-users-test1.xml";
    final Path filePath = Paths.get(path);
    Files.copy(Paths.get(origPathStr), filePath, StandardCopyOption.REPLACE_EXISTING);
    return filePath;
  }

  @Before
  public void setUp() throws Exception {
    this.fileWatcher = new FileWatcher(SensitivityWatchEventModifier.HIGH);
    PATH = setPath();
  }

  @After
  public void tearDown() throws Exception {
    if (this.fileWatcher != null) {
      this.fileWatcher.close();
    }
  }

  @Test
  public void registerAndTake() throws Exception {
    write();

    // start watching
    final Path dir = Paths.get(PATH.toString()).getParent();
    this.fileWatcher.register(dir);

    // sleep for a second to have different modification time
    Thread.sleep(1000L);
    write();

    final WatchKey key = this.fileWatcher.take();
    final List<WatchEvent<?>> events = this.fileWatcher.pollEvents(key);
    // depending on the OS & file system there may be at least 1 or 2 events even with 1 write()
    assertTrue(events.stream()
        .map(event -> ((WatchEvent<Path>) event).context())
        .map(dir::resolve)
        .anyMatch(PATH::equals));
  }

  private void write() throws IOException {
    // Update the file
    Files.write(PATH, new ArrayList<>());
  }

}
