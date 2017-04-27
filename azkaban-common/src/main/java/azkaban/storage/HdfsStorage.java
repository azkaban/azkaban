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

import azkaban.AzkabanCommonModuleConfig;
import azkaban.spi.Storage;
import azkaban.spi.StorageMetadata;
import com.google.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static azkaban.Constants.ConfigurationKeys.*;


public class HdfsStorage implements Storage {
  private final URI rootUri;
  private final FileSystem fs;

  @Inject
  public HdfsStorage(FileSystem fs, AzkabanCommonModuleConfig config) {
    this.rootUri = config.getHdfsRootUri();
    this.fs = fs;
  }

  @Override
  public InputStream get(String key) throws IOException {
    return throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public String put(StorageMetadata metadata, File localFile) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public boolean delete(String key) {
    throw new UnsupportedOperationException("Method not implemented");
  }
}
