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

import com.google.common.base.Throwables;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;


public class ProjectDependencyManager {
  private static final AtomicInteger count = new AtomicInteger(1);

  private final ProjectManagerConfig config;
  private final DependencyFetcher fetcher;

  public ProjectDependencyManager(ProjectManagerConfig config, DependencyFetcher fetcher) {
    this.config = config;
    this.fetcher = fetcher;
  }

  public void process(ProjectSpec spec) {
    process(spec.getPreExec());
  }

  public void process(ProjectSpec.PreExecutionSpec preExec) {
    fetchDependencies(preExec.getFetch().values());
  }

  /**
   * Download each URI to tmpDir and make it available to feed it to Storage.
   *
   * This is currently
   *    single threaded and linear. TODO Parallelize downloads and processing!
   *    works only with URLs. TODO Possible feature: Add artifact fetching
   *
   * @param uris Collection of URIs to be fetched.
   */
  public void fetchDependencies(Collection<URI> uris) {
    for (URI uri : uris) {
      try {
        fetcher.fetchDependency(uri);
      } catch (IOException e) {
        Throwables.propagate(e);
      }
    }
  }
}
