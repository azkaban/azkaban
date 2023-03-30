/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.spi;

import azkaban.utils.HashUtils;
import azkaban.utils.InvalidHashException;
import java.io.File;
import java.util.Map;
import java.util.Objects;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Representation of startup dependency. Maps 1:1 to an entry in startup-dependencies.json for thin
 * archives. Will automatically validate SHA1 checksum upon instantiation to avoid SQL injection
 * when this checksum is used for DB queries, as well as mitigating other issues down the road.
 */
public class Dependency {

  private final String fileName;
  private final String destination;
  private final String type;
  private final String ivyCoordinates;
  private final String sha1;

  public Dependency(final String fileName, final String destination, final String type,
      final String ivyCoordinates, final String sha1) throws InvalidHashException {
    this.fileName = fileName;
    this.destination = destination;
    this.type = type;
    this.ivyCoordinates = ivyCoordinates;
    this.sha1 = HashUtils.SHA1.sanitizeHashStr(sha1);
  }

  public Dependency(final Map<String, String> fieldMap) throws InvalidHashException {
    this(fieldMap.get("file"), fieldMap.get("destination"), fieldMap.get("type"),
        fieldMap.get("ivyCoordinates"), fieldMap.get("sha1"));
  }

  /**
   * Make a copy of this dependency
   *
   * @return a copy of this dependency
   */
  public Dependency copy() {
    try {
      return new Dependency(getFileName(), getDestination(), getType(), getIvyCoordinates(),
          getSHA1());
    } catch (final InvalidHashException e) {
      // This should never happen because we already validated the hash when creating this dependency
      throw new RuntimeException("InvalidHashException when copying dependency.");
    }
  }

  /**
   * Make a new DependencyFile with the same details as this dependency
   *
   * @param file for DependencyFile
   * @return the new DependencyFile
   */
  public DependencyFile makeDependencyFile(final File file) {
    try {
      return new DependencyFile(file, getFileName(), getDestination(), getType(),
          getIvyCoordinates(), getSHA1());
    } catch (final InvalidHashException e) {
      // This should never happen because we already validated the hash when creating this dependency
      throw new RuntimeException("InvalidHashException when copying dependency.");
    }
  }

  // it makes much more sense for the getter to be getFileName vs getFile, but in the startup-dependencies.json
  // spec we expect the property to be "file" not "fileName" so we have to annotate this to tell the JSON serializer
  // to insert it with "file", instead of assuming the name based on the name of the getter like it usually does.
  @JsonProperty("file")
  public String getFileName() {
    return this.fileName;
  }

  public String getDestination() {
    return this.destination;
  }

  public String getType() {
    return this.type;
  }

  public String getIvyCoordinates() {
    return this.ivyCoordinates;
  }

  public String getSHA1() {
    return this.sha1;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Dependency that = (Dependency) o;
    return this.fileName.equals(that.fileName) &&
        this.destination.equals(that.destination) &&
        this.type.equals(that.type) &&
        this.ivyCoordinates.equals(that.ivyCoordinates) &&
        this.sha1.equals(that.sha1);
  }

  @Override
  public String toString() {
    return this.fileName + "/" + this.destination;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.sha1);
  }
}
