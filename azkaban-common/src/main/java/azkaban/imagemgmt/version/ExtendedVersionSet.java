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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * The class extends VersionSet by capturing attributes which don't belong directly in the
 * VersionSet (and the corresponding {@code versionSetJsonString}). Image-path is an example of
 * such an attribute.
 */
public class ExtendedVersionSet extends VersionSet {

  private final Map<String, ImageVersionPath> imageVersionPaths;

  // Visibility is package private to make this accessible within loaders/builders in this package.
  ExtendedVersionSet(VersionSet versionSet,
      Map<String, ImageVersionPath> imageVersionPaths) throws IOException {
    super(versionSet);
    this.imageVersionPaths = imageVersionPaths;
  }

  /**
   * Return image version and path of the image identified by {@code imageType}
   * @param imageType
   * @return
   */
  public Optional<ImageVersionPath> getVersionPath(final String imageType) {
    return Optional.ofNullable(this.imageVersionPaths.get(imageType.toLowerCase()));
  }

  public static class ImageVersionPath {
    private final String imageType;
    private final String imageVersion;
    private final String imagePath;

    public ImageVersionPath(String imageType, String imageVersion, String imagePath) {
      this.imageType = imageType;
      this.imageVersion = imageVersion;
      this.imagePath = imagePath;
    }

    public String getImageType() {
      return imageType;
    }

    public String getImageVersion() {
      return imageVersion;
    }

    public String getImagePath() {
      return imagePath;
    }

    public String pathWithVersion() {
      Preconditions.checkNotNull(imagePath, "image path should not be null");
      Preconditions.checkNotNull(imageVersion, "image version should not be null");
      return String.join(":", imagePath, imageVersion);
    }
  }
}
