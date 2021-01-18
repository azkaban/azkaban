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
package azkaban.imagemgmt.models;

import java.util.List;

/**
 * This represents image version metadata such as version details, rampup information etc.
 */
public class ImageVersionMetadata {

  private final ImageVersion imageVersion;
  private List<ImageRampup> imageRampups;

  public ImageVersionMetadata(final ImageVersion imageVersion) {
    this.imageVersion = imageVersion;
  }

  public ImageVersionMetadata(final ImageVersion imageVersion, final List<ImageRampup> imageRampups) {
    this.imageVersion = imageVersion;
    this.imageRampups = imageRampups;
  }

  public ImageVersion getImageVersion() {
    return this.imageVersion;
  }

  public List<ImageRampup> getImageRampups() {
    return this.imageRampups;
  }
}
