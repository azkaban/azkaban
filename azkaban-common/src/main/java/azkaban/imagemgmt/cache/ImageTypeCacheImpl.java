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
package azkaban.imagemgmt.cache;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_IMAGE_TYPE_CACHE_MAX_SIZE;

import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageType;
import azkaban.utils.Props;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation for image type cache. The implementation is based on LRU cache. The cache key
 * is image type name and value is image type metadata including ownerships.
 */
@Singleton
public class ImageTypeCacheImpl extends LRUCache<String, ImageType> implements ImageTypeCache {

  private static final Logger log = LoggerFactory.getLogger(ImageTypeCacheImpl.class);

  private final ImageTypeDao imageTypeDao;

  @Inject
  public ImageTypeCacheImpl(final Props props, final ImageTypeDao imageTypeDao) {
    super(props.getInt(AZKABAN_IMAGE_TYPE_CACHE_MAX_SIZE, 100));
    this.imageTypeDao = imageTypeDao;
    initialize();
  }

  /**
   * Populate the cache by loading image type metadata.
   */
  private void initialize() {
    final List<ImageType> imageTypes = getAllImageTypesWithOwnerships();
    for (final ImageType imageType : imageTypes) {
      getCache().put(imageType.getName(), imageType);
    }
  }

  @Override
  public Optional<ImageType> getImageTypeWithOwnershipsByName(final String name)
      throws ImageMgmtException {
    final ImageType imageType = getCache().get(name);
    if (imageType == null) {
      final Optional<ImageType> optionalImageType = imageTypeDao
          .getImageTypeWithOwnershipsByName(name);
      if (optionalImageType.isPresent()) {
        getCache().put(name, optionalImageType.get());
      }
      return optionalImageType;
    }
    return Optional.ofNullable(imageType);
  }

  /**
   * Gets all image type metadata using the image type dao.
   * @return List<ImageType>
   * @throws ImageMgmtException
   */
  private List<ImageType> getAllImageTypesWithOwnerships() throws ImageMgmtException {
    return imageTypeDao.getAllImageTypesWithOwnerships();
  }
}
