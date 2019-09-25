package azkaban.spi;

import azkaban.utils.InvalidHashException;
import java.io.File;
import java.util.Objects;

/**
 * Representation of startup dependency with an associated local file. Usually a DependencyFile will never be
 * directly instantiated (except maybe in tests), but rather will be generated from an instance of a Dependency
 * using Dependency::makeDependencyFile(File f)
 */
public class DependencyFile extends Dependency {
  private File file;

  // NOTE: This should NEVER throw InvalidHashException because the input dependency
  // must have already had its cache validated upon instantiation.
  public DependencyFile(File f, String fileName, String destination, String type, String ivyCoordinates, String sha1)
      throws InvalidHashException {
    super(fileName, destination, type, ivyCoordinates, sha1);
    this.file = f;
  }

  public File getFile() { return this.file; }
  public void setFile(File file) { this.file = file; }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    DependencyFile that = (DependencyFile) o;
    return Objects.equals(file, that.file);
  }
}
