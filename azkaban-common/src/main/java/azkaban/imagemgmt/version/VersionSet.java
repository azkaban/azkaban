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

/**
 * VersionSet is defined as the set of key-val pairs to uniquely define the execution environment.
 * This version set will have azkaban version, azkaban configuration version, job type versions,
 * etc.
 * <p>
 * Example version set json: {"azConf":"0.0.5","azkaban":"0.0.2","sparkJobType":"0.1.2"}
 */
public class VersionSet {

  private final String versionSetJsonString;
  private final String versionSetMd5Hex;
  private final int versionSetId;

  public VersionSet(String versionSetJsonString, String versionSetMd5Hex, int versionSetId) {
    this.versionSetJsonString = versionSetJsonString;
    this.versionSetMd5Hex = versionSetMd5Hex;
    this.versionSetId = versionSetId;
  }

  public String getVersionSetJsonString() {
    return this.versionSetJsonString;
  }

  public String getVersionSetMd5Hex() {
    return this.versionSetMd5Hex;
  }

  public int getVersionSetId() {
    return this.versionSetId;
  }
}
