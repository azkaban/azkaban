package azkaban.execapp.projectcache;

import azkaban.execapp.projectcache.ProjectCacheKey;
import java.io.File;

public class ProjectDirectoryInfo {
  private final ProjectCacheKey key;
  private final File directory;
  private final long sizeInBytes;

  public ProjectDirectoryInfo(ProjectCacheKey key, File directory, long sizeInBytes) {
    this.key = key;
    this.directory = directory;
    this.sizeInBytes = sizeInBytes;
  }

  public ProjectCacheKey getKey() {
    return key;
  }

  public File getDirectory() {
    return directory;
  }

  public long getSizeInBytes() {
    return sizeInBytes;
  }
}
