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
 * Represents image version and its associated metadata.
 */
public class ImageVersionUsageData {

  private final ImageVersion imageVersion;
  private final List<ImageRampupPlan> imageRampupPlans;

  public ImageVersionUsageData(final ImageVersion imageVersion,
      final List<ImageRampupPlan> imageRampupPlans) {
    this.imageVersion = imageVersion;
    this.imageRampupPlans = imageRampupPlans;
  }

  public ImageVersion getImageVersion() {
    return this.imageVersion;
  }

  public List<ImageRampupPlan> getImageRampupPlans() {
    return this.imageRampupPlans;
  }
}
