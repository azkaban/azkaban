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

import java.net.URI;


/**
 * This exception is thrown when there is an attempt to create a duplicate storage key via the {@link Storage}
 * interface.
 */
public class KeyAlreadyExistsException extends StorageException {
  public KeyAlreadyExistsException(URI key) {
    super("Storage key already present: " + key);
  }

}
