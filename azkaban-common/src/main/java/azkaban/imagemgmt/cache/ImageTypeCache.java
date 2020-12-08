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

import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageType;
import java.util.Optional;

/**
 * Interface exposing methods for image type cache implementation.
 */
public interface ImageTypeCache extends BaseCache<String, ImageType> {

  /**
   * Method to get image type with ownerships based on image type name.
   * @param name
   * @return Optional<ImageType>
   * @throws ImageMgmtException
   */
  public Optional<ImageType> getImageTypeWithOwnershipsByName(String name)
      throws ImageMgmtException;
}
