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

  /**
   * Get an InputStream object by providing a key.
   *
   * @param key The key is a string pointing to the blob in Storage.
   * @return InputStream for fetching the blob. null if the key is not found.
   */
  InputStream get(String key) throws IOException;

  /**
   * Put an object and return a key.
   *
   * @param metadata Metadata related to the input stream
   * @param localFile Read data from a local file
   * @return Key associated with the current object on successful put
   */
  String put(StorageMetadata metadata, File localFile);

  /**
   * Delete an object from Storage.
   *
   * @param key The key is a string pointing to the blob in Storage.
   * @return true if delete was successful. false if there was nothing to delete.
   */
  boolean delete(String key);
}
