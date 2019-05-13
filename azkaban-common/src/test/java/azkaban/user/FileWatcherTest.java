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

import static org.junit.Assert.assertEquals;

import com.sun.nio.file.SensitivityWatchEventModifier;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileWatcherTest {

  private static final Path PATH = Paths.get("build/tmp/FileWatcherTest");
  private FileWatcher fileWatcher;

  @Before
  public void setUp() throws Exception {
    this.fileWatcher = new FileWatcher(SensitivityWatchEventModifier.HIGH);
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
    assertEquals(1, events.size());

    final WatchEvent<?> event = events.get(0);
    @SuppressWarnings("unchecked") final Path name = ((WatchEvent<Path>) event).context();
    final String resolvedFileName = dir.resolve(name).toString();
    assertEquals(PATH.toString(), resolvedFileName);
  }

  private void write() throws IOException {
    // Update the file
    Files.write(PATH, new ArrayList<>());
  }

}
