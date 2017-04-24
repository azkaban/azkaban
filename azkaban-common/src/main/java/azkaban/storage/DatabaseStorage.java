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

import azkaban.project.ProjectLoader;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.spi.StorageMetadata;
import com.google.common.base.Splitter;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import javax.inject.Inject;
import org.apache.http.client.utils.URIBuilder;


/**
 * DatabaseStorage
 *
 * This class helps in storing projects in the DB itself. This is intended to be the default since it is the current
 * behavior of Azkaban.
 */
public class DatabaseStorage implements Storage {
  public static final String PROJECT_ID = "projectId";
  public static final String VERSION = "version";

  private final ProjectLoader projectLoader;

  @Inject
  public DatabaseStorage(ProjectLoader projectLoader) {

    this.projectLoader = projectLoader;
  }

  public static URI toURI(int projectId, int version) {
    try {
      return new URIBuilder()
          .setScheme("database")
          .setPath("/") // required. inserting dummy path "/". else query parsing does not work.
          .setParameter(PROJECT_ID, String.valueOf(projectId))
          .setParameter(VERSION, String.valueOf(version))
          .build();
    } catch (URISyntaxException e) {
      throw new StorageException(e);
    }
  }

  public static Map<String, String> getQueryMapFromUri(URI uri) {
    return Splitter.on('&')
          .trimResults()
          .withKeyValueSeparator("=")
          .split(uri.getQuery());
  }

  @Override
  public InputStream get(URI key) {
    Map<String, String> queryMap = getQueryMapFromUri(key);
    final int projectId = Integer.valueOf(queryMap.get(PROJECT_ID));
    final int version = Integer.valueOf(queryMap.get(VERSION));

    projectLoader.getUploadedFile(projectId, version);
    return null;
  }

  @Override
  public URI put(StorageMetadata metadata, File localFile) {
    projectLoader.uploadProjectFile(
        metadata.getProjectId(),
        metadata.getVersion(),
        localFile, metadata.getUploader());

    return null;
  }

  @Override
  public boolean delete(URI key) {
    throw new UnsupportedOperationException("Delete is not supported");
  }
}
