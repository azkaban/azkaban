/*
 * Copyright 2020 LinkedIn Corp.
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

package azkaban.imagemgmt.version;

import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.version.ExtendedVersionSet.ImageVersionPath;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.collections4.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ExtendedVersionSetLoader {
  private static final Logger logger = LoggerFactory.getLogger(ExtendedVersionSetLoader.class);

  private final ImageVersionDao imageVersionDao;

  @Inject
  public ExtendedVersionSetLoader(ImageVersionDao imageVersionDao) {
    this.imageVersionDao = imageVersionDao;
  }

  public ExtendedVersionSet createExtendedVersionSet(VersionSet versionSet)
      throws IOException {
    return new ExtendedVersionSet(versionSet,
        imageVersionPathsFromMap(versionSet.getImageToVersionMap()));
  }

  // todo: This currently requires a sql query for every image-type in the version set.
  // Update ImageVersionDao such that a single query can fetch all the needed values.
  private Map<String, ImageVersionPath> imageVersionPathsFromMap(
      SortedMap<String, String> imageToVersionMap) {
    Map<String, ImageVersionPath> imageVersionPathMap = new HashedMap<>();
    for(Map.Entry<String, String>  entry: imageToVersionMap.entrySet()) {
      Optional<ImageVersion> imageVersion = imageVersionDao.getImageVersion(entry.getKey(),
          entry.getValue());
      if (!imageVersion.isPresent()) {
        logger.warn("Image version could not be located for image-type: {}, version: {} ",
            entry.getKey(), entry.getValue());
      }
      imageVersionPathMap.put(entry.getKey(), new ImageVersionPath(entry.getKey(),
          entry.getValue(), imageVersion.get().getPath()));
    }
    return imageVersionPathMap;
  }
}
