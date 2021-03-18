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

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;

/**
 * This class represents rampup information for an image type.
 */
public class ImageRampup extends BaseModel {

  // Represents rampup plan id
  private int planId;
  // Image version
  private String imageVersion;
  // Image version rampup percentage
  private Integer rampupPercentage;
  // Stability tag of the version being ramped up
  private StabilityTag stabilityTag;
  // Release tag of the version being ramped up
  private String releaseTag;

  public void setReleaseTag(String releaseTag) {
    this.releaseTag = releaseTag;
  }

  public String getReleaseTag() {
    return this.releaseTag;
  }

  public int getPlanId() {
    return planId;
  }

  public void setPlanId(int planId) {
    this.planId = planId;
  }

  public String getImageVersion() {
    return imageVersion;
  }

  public void setImageVersion(String imageVersion) {
    this.imageVersion = imageVersion;
  }

  public Integer getRampupPercentage() {
    return rampupPercentage;
  }

  public void setRampupPercentage(Integer rampupPercentage) {
    this.rampupPercentage = rampupPercentage;
  }

  public StabilityTag getStabilityTag() {
    return stabilityTag;
  }

  public void setStabilityTag(StabilityTag stabilityTag) {
    this.stabilityTag = stabilityTag;
  }

  /**
   * Enum representing the stability of the version being rampup. By default when an image version
   * is picked up for rampup the version will be marked as "experimental" in the image_rampup table.
   * During rampup if there is no issue with the version, the version can be updated as "stable" in
   * the image_rampup table. And if there are issues, the version will be updated as "unstable" in
   * the image_rampup table. Azkaban developer or image type owner can mark the version in the plan
   * as "stable" or "unstable" using update rampup API.
   */
  public enum StabilityTag {
    EXPERIMENTAL("experimental"),
    STABLE("stable"),
    UNSTABLE("unstable");

    private final String tagName;

    private static final ImmutableMap<String, StabilityTag> tagMap =
        Arrays.stream(StabilityTag.values())
            .collect(ImmutableMap.toImmutableMap(tag -> tag.getTagName(), tag -> tag));

    private StabilityTag(String tagName) {
      this.tagName = tagName;
    }

    public String getTagName() {
      return tagName;
    }

    public static StabilityTag fromTagName(String tagName) {
      return tagMap.getOrDefault(tagName, EXPERIMENTAL);
    }
  }

  @Override
  public String toString() {
    return "ImageRampup{" +
        "planId=" + planId +
        ", imageVersion='" + imageVersion + '\'' +
        ", stabilityTag=" + stabilityTag +
        ", id=" + id +
        ", createdBy='" + createdBy + '\'' +
        ", createdOn='" + createdOn + '\'' +
        ", modifiedBy='" + modifiedBy + '\'' +
        ", modifiedOn='" + modifiedOn + '\'' +
        '}';
  }
}
