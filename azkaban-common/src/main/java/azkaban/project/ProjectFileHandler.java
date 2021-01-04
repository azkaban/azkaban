/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.project;

import azkaban.spi.Dependency;
import java.io.File;
import java.util.Set;


public class ProjectFileHandler {

  private final int projectId;
  private final int version;
  private final long uploadTime;
  private final String fileType;
  private final String fileName;
  private final String uploader;
  private final byte[] MD5Hash;
  private final int numChunks;
  private final Set<Dependency> startupDependencies;
  private final String resourceId;
  private final String uploaderIpAddr;

  private File localFile = null;

  public ProjectFileHandler(
      final int projectId,
      final int version,
      final long uploadTime,
      final String uploader,
      final String fileType,
      final String fileName,
      final int numChunks,
      final byte[] MD5Hash,
      final Set<Dependency> startupDependencies,
      final String resourceId,
      final String uploaderIpAddr) {
    this.projectId = projectId;
    this.version = version;
    this.uploadTime = uploadTime;
    this.uploader = uploader;
    this.fileType = fileType;
    this.fileName = fileName;
    this.MD5Hash = MD5Hash;
    this.numChunks = numChunks;
    this.startupDependencies = startupDependencies;
    this.resourceId = resourceId;
    this.uploaderIpAddr = uploaderIpAddr;
  }

  public int getProjectId() {
    return this.projectId;
  }

  public int getVersion() {
    return this.version;
  }

  public long getUploadTime() {
    return this.uploadTime;
  }

  public String getFileType() {
    return this.fileType;
  }

  public String getFileName() {
    return this.fileName;
  }

  public byte[] getMD5Hash() {
    return this.MD5Hash;
  }

  public File getLocalFile() {
    return this.localFile;
  }

  public synchronized void setLocalFile(final File localFile) {
    this.localFile = localFile;
  }

  public synchronized void deleteLocalFile() {
    if (this.localFile != null) {
      this.localFile.delete();
      this.localFile = null;
    }
  }

  public String getUploader() {
    return this.uploader;
  }

  public int getNumChunks() {
    return this.numChunks;
  }

  public Set<Dependency> getStartupDependencies() { return this.startupDependencies; }

  public String getResourceId() {
    return this.resourceId;
  }

  public String getUploaderIpAddr() {
    return this.uploaderIpAddr;
  }
}
