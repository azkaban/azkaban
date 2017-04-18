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

package azkaban.storage;

import azkaban.project.JdbcProjectLoader;
import azkaban.spi.Storage;
import azkaban.spi.StorageMetadata;
import java.io.InputStream;
import java.net.URI;
import javax.inject.Inject;


/**
 * DatabaseStorage
 *
 * This class helps in storing projects in the DB itself. This is intended to be the default since it is the current
 * behavior of Azkaban.
 */
public class DatabaseStorage implements Storage {

  @Inject
  public DatabaseStorage(JdbcProjectLoader jdbcProjectLoader) {

  }

  @Override
  public InputStream get(URI key) {
    return null;
  }

  @Override
  public URI put(StorageMetadata metadata, InputStream is) {
    return null;
  }

  @Override
  public boolean delete(URI key) {
    throw new UnsupportedOperationException("Delete is not supported");
  }
}
