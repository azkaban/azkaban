/*
 * Copyright 2021 LinkedIn Corp.
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
package azkaban.imagemgmt.models;

import java.util.List;

/**
 * This represents image version metadata such as version details, rampup information etc.
 */
public class ImageVersionMetadata {

  private final ImageVersion imageVersion;
  private List<ImageRampup> imageRampups;
  // Captures the information about version selection process such as the version is based on
  // either random rampup or based on latest available active version.
  private final String message;

  public ImageVersionMetadata(final ImageVersion imageVersion, final String message) {
    this.imageVersion = imageVersion;
    this.message = message;
  }

  public ImageVersionMetadata(final ImageVersion imageVersion,
      final List<ImageRampup> imageRampups, String message) {
    this.imageVersion = imageVersion;
    this.imageRampups = imageRampups;
    this.message = message;
  }

  public ImageVersion getImageVersion() {
    return this.imageVersion;
  }

  public List<ImageRampup> getImageRampups() {
    return this.imageRampups;
  }

  public String getMessage() {
    return message;
  }
}
