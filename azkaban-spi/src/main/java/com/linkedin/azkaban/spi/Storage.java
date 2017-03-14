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

package com.linkedin.azkaban.spi;

import java.io.InputStream;
import java.net.URI;


/**
 * The Azkaban Storage interface would facilitate getting and putting objects into a storage mechanism of choice.
 * By default, this is set to the MySQL database. However, users can have the ability to choose between multiple
 * storage types in future.
 *
 * This is different from storing Azkaban state in MySQL which would typically be maintained in a different database.
 *
 * Note: This is a synchronous interface.
 */
public interface Storage {

  /**
   * Check if key exists in storage.
   *
   * @param key The key is a URI pointing to the blob in Storage.
   * @return true if key exists. false otherwise.
   */
  boolean containsKey(URI key);

  /**
   * Get an InputStream object by providing a key. Throws {@link KeyDoesNotExistException} if key is not present.
   *
   * @param key The key is a URI pointing to the blob in Storage.
   * @return InputStream for fetching the blob.
   *
   */
  InputStream get(URI key) throws KeyDoesNotExistException;

  /**
   * Put an object into Storage against a key. If the key already exists, then it throws
   * {@link KeyAlreadyExistsException;}.
   *
   * @param key The key is a URI pointing to the blob in Storage.
   * @param is The input stream from which the value is read to the store. The value is read completely
   */
  void put(URI key, InputStream is) throws KeyAlreadyExistsException;

  /**
   * Delete an object from Storage. Throws {@link KeyDoesNotExistException} if key is not present.
   *
   * @param key The key is a URI pointing to the blob in Storage.
   */
  void delete(URI key) throws KeyDoesNotExistException;
}
