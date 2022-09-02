/*
 * Copyright 2022 LinkedIn Corp.
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

import java.util.Set;
import javax.annotation.Nullable;


/**
 * This class represents Image Ramp Rule metadata.
 */
public class ImageRampRule extends BaseModel {
  // ramp rule name
  String ruleName;
  // image name of ramp rule for
  String imageName;
  // image version of ramp rule for
  String imageVersion;
  // rule owners
  Set<String> owners;
  // Ramp Rule for HP flows
  // ture if rule is for HP flow; false otherwise
  boolean isHPRule;

  public ImageRampRule(String ruleName,
                String imageName,
                String imageVersion,
                Set<String> owners,
                boolean isHP,
                String createdBy,
                @Nullable String createdOn ,
                @Nullable String modifiedBy,
                @Nullable String modifiedOn) {
    this.ruleName = ruleName;
    this.imageName = imageName;
    this.imageVersion = imageVersion;
    this.owners = owners;
    this.isHPRule = isHP;
    super.createdBy = createdBy;
    super.createdOn = createdOn;
    super.modifiedBy = modifiedBy;
    super.modifiedOn = modifiedOn;
  }

  public String getRuleName() {
    return ruleName;
  }

  public String getImageName() {
    return imageName;
  }

  public String getImageVersion() {
    return imageVersion;
  }

  public String getOwners() {
    return String.join(",", owners);
  }

  public boolean isHPRule() {
    return isHPRule;
  }

  public static class Builder {
    // ramp rule name
    String ruleName;
    // image name of ramp rule for
    String imageName;
    // image version of ramp rule for
    String imageVersion;
    // rule owners
    Set<String> owners;
    // Ramp Rule for HP flows or not
    boolean isHPRule;
    // Created by user
    String createdBy;
    // Created Time
    String createdOn;
    // Modified by user
    String modifiedBy;
    // Modified Time
    String modifiedOn;

    public Builder setRuleName(String ruleName) {
      this.ruleName = ruleName;
      return this;
    }

    public Builder setImageName(String imageName) {
      this.imageName = imageName;
      return this;
    }

    public Builder setImageVersion(String imageVersion) {
      this.imageVersion = imageVersion;
      return this;
    }

    public Builder setOwners(Set<String> owners) {
      this.owners = owners;
      return this;
    }

    public Builder setHPRule(boolean HPRule) {
      isHPRule = HPRule;
      return this;
    }

    public Builder setCreatedBy(String createdBy) {
      this.createdBy = createdBy;
      return this;
    }

    public Builder setCreatedOn(String createdOn) {
      this.createdOn = createdOn;
      return this;
    }

    public Builder setModifiedBy(String modifiedBy) {
      this.modifiedBy = modifiedBy;
      return this;
    }

    public Builder setModifiedOn(String modifiedOn) {
      this.modifiedOn = modifiedOn;
      return this;
    }

    public ImageRampRule build() {
      return new ImageRampRule(ruleName, imageName, imageVersion, owners, isHPRule,
         createdBy, createdOn, modifiedBy, modifiedOn);
    }

  }
}
