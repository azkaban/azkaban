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

package azkaban.spi;

import azkaban.utils.InvalidHashException;
import java.io.File;
import java.util.Objects;

/**
 * Representation of startup dependency with an associated local file. Usually a DependencyFile will never be
 * directly instantiated (except maybe in tests), but rather will be generated from an instance of a Dependency
 * using Dependency::makeDependencyFile(File f)
 */
public class DependencyFile extends Dependency {
  private File file;

  // NOTE: This should NEVER throw InvalidHashException because the input dependency
  // must have already had its cache validated upon instantiation.
  public DependencyFile(final File f, final String fileName, final String destination, final String type,
      final String ivyCoordinates, final String sha1) throws InvalidHashException {
    super(fileName, destination, type, ivyCoordinates, sha1);
    this.file = f;
  }

  public File getFile() { return this.file; }
  public void setFile(File file) { this.file = file; }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    DependencyFile that = (DependencyFile) o;
    return Objects.equals(file, that.file);
  }
}
