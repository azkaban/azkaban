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

import azkaban.imagemgmt.daos.RampRuleDao.DenyMode;
import javax.annotation.Nullable;

/**
 * This class represents flows with excluded executable mode or image versions from image ramp rules.
 * */
public class RampRuleDenyList extends BaseModel {

  // in the format of projectName.flowName
  final String flowId;
  // deny mode for current flow, see explanation in {@link DenyMode}
  final DenyMode denyMode;
  // deny versions, only exists when DenyMode is PARTIAL
  @Nullable
  String denyVersion;
  // rule name that regulates this flow deny list
  final String ruleName;

  public RampRuleDenyList(final String flowId,
      final DenyMode denyMode,
      @Nullable String denyVersion,
      final String ruleName,
      final String createdBy,
      final String createdOn ,
      final String modifiedBy,
      final String modifiedOn) {
    this.ruleName = ruleName;
    this.flowId = flowId;
    this.denyMode = denyMode;
    this.denyVersion = denyVersion;
    super.createdBy = createdBy;
    super.createdOn = createdOn;
    super.modifiedBy = modifiedBy;
    super.modifiedOn = modifiedOn;
  }

  public String getRuleName() {
    return ruleName;
  }

  public String getFlowId() {
    return flowId;
  }

  public DenyMode getDenyMode() {
    return denyMode;
  }

  @Nullable
  public String getDenyVersion() {
    return denyVersion;
  }

  public static class Builder {
    // in the format of projectName.flowName
    String flowId;
    // deny mode for current flow, see explanation in {@link DenyMode}
    DenyMode denyMode;
    // deny versions, only exists when DenyMode is PARTIAL
    String denyVersion;
    // rule name that regulates this flow deny list
    String ruleName;
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

    public Builder setFlowId(String imageName) {
      this.flowId = imageName;
      return this;
    }

    public Builder setDenyMode(String denyMode) {
      this.denyMode = DenyMode.valueOf(denyMode);
      return this;
    }

    public Builder setDenyVersion(String denyVersion) {
      this.denyVersion = denyVersion;
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

    public RampRuleDenyList build() {
      return new RampRuleDenyList(flowId, denyMode, denyVersion, ruleName,
          createdBy, createdOn, modifiedBy, modifiedOn);
    }
  }
}

