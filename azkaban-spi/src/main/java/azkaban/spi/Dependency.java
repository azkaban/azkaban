package azkaban.spi;

import azkaban.utils.HashUtils;
import azkaban.utils.InvalidHashException;
import java.io.File;
import java.util.Map;
import java.util.Objects;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Representation of startup dependency. Maps 1:1 to an entry in startup-dependencies.json for thin archives.
 * Will automatically validate SHA1 checksum upon instantiation to avoid SQL injection when this checksum is used
 * for DB queries, as well as mitigating other issues down the road.
 */
public class Dependency {
  private final String fileName;
  private final String destination;
  private final String type;
  private final String ivyCoordinates;
  private final String sha1;

  public Dependency(String fileName, String destination, String type, String ivyCoordinates, String sha1)
      throws InvalidHashException {
    this.fileName = fileName;
    this.destination = destination;
    this.type = type;
    this.ivyCoordinates = ivyCoordinates;
    this.sha1 = HashUtils.SHA1.sanitizeHashStr(sha1);
  }

  public Dependency(Map<String, String> m) throws InvalidHashException {
    this(m.get("file"), m.get("destination"), m.get("type"), m.get("ivyCoordinates"), m.get("sha1"));
  }

  /**
   * Make a copy of this dependency
   *
   * @return a copy of this dependency
   */
  public Dependency makeCopy() {
    try {
      return new Dependency(getFileName(), getDestination(), getType(), getIvyCoordinates(), getSHA1());
    } catch (InvalidHashException e) {
      // This should never happen because we already validated the hash when creating this dependency
      throw new RuntimeException("InvalidHashException when copying dependency.");
    }
  }

  /**
   * Make a new DependencyFile with the same details as this dependency
   *
   * @param f file for DependencyFile
   * @return the new DependencyFile
   */
  public DependencyFile makeDependencyFile(File f) {
    try {
      return new DependencyFile(f, getFileName(), getDestination(), getType(), getIvyCoordinates(), getSHA1());
    } catch (InvalidHashException e) {
      // This should never happen because we already validated the hash when creating this dependency
      throw new RuntimeException("InvalidHashException when copying dependency.");
    }
  }

  // it makes much more sense for the getter to be getFileName vs getFile, but in the startup-dependencies.json
  // spec we expect the property to be "file" not "fileName" so we have to annotate this to tell the JSON serializer
  // to insert it with "file", instead of assuming the name based on the name of the getter like it usually does.
  @JsonProperty("file")
  public String getFileName() { return fileName; }

  public String getDestination() { return destination; }
  public String getType() { return type; }
  public String getIvyCoordinates() { return ivyCoordinates; }
  public String getSHA1() { return sha1; }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Dependency that = (Dependency) o;
    return fileName.equals(that.fileName) && type.equals(that.type) && ivyCoordinates.equals(that.ivyCoordinates)
        && sha1.equals(that.sha1);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sha1);
  }
}
