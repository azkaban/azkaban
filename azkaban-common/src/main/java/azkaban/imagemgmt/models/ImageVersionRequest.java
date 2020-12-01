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

import azkaban.imagemgmt.models.ImageVersion.State;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This is image version request class for updating image version metadata.
 */
public class ImageVersionRequest extends BaseModel {

  @JsonProperty("imagePath")
  private String path;
  // Description of the image version
  private String description;
  // State of the image version
  @JsonProperty("versionState")
  private State state;

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public State getState() {
    return state;
  }

  public void setState(State state) {
    this.state = state;
  }

  @Override
  public String toString() {
    return "ImageVersionUpdateRequest{" +
        "path='" + path + '\'' +
        ", description='" + description + '\'' +
        ", state=" + state +
        ", id=" + id +
        ", createdBy='" + createdBy + '\'' +
        ", createdOn='" + createdOn + '\'' +
        ", modifiedBy='" + modifiedBy + '\'' +
        ", modifiedOn='" + modifiedOn + '\'' +
        '}';
  }
}
