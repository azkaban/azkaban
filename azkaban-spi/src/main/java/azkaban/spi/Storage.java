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

package azkaban.spi;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;


/**
 * The Azkaban Storage interface would facilitate getting and putting objects into a storage
 * mechanism of choice. By default, this is set to the MySQL database. However, users can have the
 * ability to choose between multiple storage types in future.
 *
 * This is different from storing Azkaban state in MySQL which would typically be maintained in a
 * different database.
 *
 * Note: This is a synchronous interface.
 */
public interface Storage {
  public static final String DEPENDENCY_STORAGE_PATH_PREFIX_PROP = "dependency.storage.path.prefix";

  /**
   * Get an InputStream object for a project by providing a key.
   *
   * @param key The key is a string pointing to the blob in Storage.
   * @return InputStream for fetching the blob. null if the key is not found.
   */
  InputStream getProject(String key) throws IOException;

  /**
   * Put a project and return a key.
   *
   * @param metadata Metadata related to the input stream
   * @param localFile Read data from a local file
   * @return Key associated with the current object on successful put
   */
  String putProject(ProjectStorageMetadata metadata, File localFile);

  /**
   * Put a dependency and return the resulting FileStatus
   *
   * @param file File and dependency details associated with dependency
   * @return FileStatus resulting file status after writing (or attempting to write)
   */
  FileIOStatus putDependency(DependencyFile file) throws IOException;

  /**
   * Get an InputStream object for a dependency.
   *
   * @param dep the dependency to fetch
   * @return InputStream for fetching the blob.
   */
  InputStream getDependency(Dependency dep) throws IOException;

  /**
   * Get the FileStatus of an dependency.
   *
   * @param dep the dependency for which to fetch the FileStatus for
   * @return current FileStatus of the dependency
   */
  FileIOStatus dependencyStatus(Dependency dep) throws IOException;

  /**
   * Delete an object from Storage.
   *
   * @param key The key is a string pointing to the blob in Storage.
   * @return true if delete was successful. false if there was nothing to delete.
   */
  boolean delete(String key);
}
