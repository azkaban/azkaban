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

import com.sun.nio.file.SensitivityWatchEventModifier;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;

public class FileWatcher {

  private final WatchService watchService;
  private final SensitivityWatchEventModifier sensitivity;

  public FileWatcher() throws IOException {
    this(SensitivityWatchEventModifier.MEDIUM);
  }

  public FileWatcher(final SensitivityWatchEventModifier sensitivity) throws IOException {
    this.sensitivity = sensitivity;
    this.watchService = FileSystems.getDefault().newWatchService();
  }

  public WatchKey register(final Path dir) throws IOException {
    return dir.register(this.watchService,
        new WatchEvent.Kind[]{StandardWatchEventKinds.ENTRY_MODIFY},
        this.sensitivity);
  }

  public void close() throws IOException {
    this.watchService.close();
  }

  public WatchKey take() throws InterruptedException {
    final WatchKey key = this.watchService.take();
    // Wait for a second to ensure there is only one event for a modification.
    // For a file update, WatchService creates two ENTRY_MODIFY events, 1 for content and 1
    // for modification time.
    // Adding the sleep consolidates both the events into one with a count of 2 which
    // avoids multiple reloads of same file.
    // One second seems excessive, however, these events happen very less often and it is
    // more important that the config reloads successfully than immediately.
    // If there is any modification happening to file(s) in the meantime, it is all queued up
    // in the watch service.
    Thread.sleep(1000L);
    return key;
  }

  public List<WatchEvent<?>> pollEvents(final WatchKey key) {
    try {
      return key.pollEvents();
    } finally {
      // continue listening – without reset() this key wouldn't be returned by take() any more
      key.reset();
    }
  }

  @FunctionalInterface
  public interface FileWatcherFactory {

    FileWatcher get() throws IOException;
  }

}
