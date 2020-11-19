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
import java.util.List;
import javax.validation.constraints.NotBlank;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This class represents image type metadata
 */
public class ImageType extends BaseModel {
  // Represents the name of the image type
  @JsonProperty("imageType")
  @NotBlank(message = "ImageType cannot be blank")
  private String name;
  // Type description
  private String description;
  // Represents the actual deployable such as image, jar tar etc.
  private Deployable deployable;
  // Associated ownership information for the image type
  private List<ImageOwnership> ownerships;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Deployable getDeployable() {
    return deployable;
  }

  public void setDeployable(Deployable deployable) {
    this.deployable = deployable;
  }

  public List<ImageOwnership> getOwnerships() {
    return ownerships;
  }

  public void setOwnerships(List<ImageOwnership> ownerships) {
    this.ownerships = ownerships;
  }

  @Override
  public String toString() {
    return "ImageType{" +
        "name='" + name + '\'' +
        ", description='" + description + '\'' +
        ", deployable=" + deployable +
        ", ownerships=" + ownerships +
        ", id=" + id +
        ", createdBy='" + createdBy + '\'' +
        ", createdOn='" + createdOn + '\'' +
        ", modifiedBy='" + modifiedBy + '\'' +
        ", modifiedOn='" + modifiedOn + '\'' +
        '}';
  }

  /**
   * Enum representing deployable
   * IMAGE - This is deployable of type image. All deployables by default are image while
   * registering image type
   * TAR - The final deployable for configs is in tar format and hence will be registered as tar
   * JAR - Placeholder for any artifacts which are available in the form of jar, can be
   * registered as deployable jar
   */
  public enum Deployable {
    IMAGE("image"),
    TAR("tar"),
    JAR("jar");

    private Deployable(String name) {
      this.name = name;
    }

    // Deployable name
    private final String name;

    // Enum name and Deployable enum map
    private static final ImmutableMap<String, Deployable> deployableMap =
        Arrays.stream(Deployable.values())
        .collect(ImmutableMap.toImmutableMap(deployable -> deployable.getName(), deployable -> deployable));

    public String getName() {
      return name;
    }

    /**
     * Creates Deployable enum for enum name
     * @param name - enum name
     * @return Deployable
     */
    public static Deployable fromDeployableName(final String name) {
      return deployableMap.getOrDefault(name, IMAGE);
    }
  }
}
