/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban.reportal.util;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StreamProviderHDFS implements IStreamProvider {

  FileSystem hdfs;
  HadoopSecurityManager securityManager;
  String username;

  public void setHadoopSecurityManager(final HadoopSecurityManager securityManager) {
    this.securityManager = securityManager;
  }

  @Override
  public void setUser(final String user) {
    this.username = user;
  }

  @Override
  public String[] getFileList(final String pathString)
      throws HadoopSecurityManagerException, IOException {
    final FileStatus[] statusList = getFileStatusList(pathString);
    final String[] fileList = new String[statusList.length];

    for (int i = 0; i < statusList.length; i++) {
      fileList[i] = statusList[i].getPath().getName();
    }

    return fileList;
  }

  @Override
  public String[] getOldFiles(final String pathString, final long thresholdTime)
      throws Exception {
    final FileStatus[] statusList = getFileStatusList(pathString);

    final List<String> oldFiles = new ArrayList<>();

    for (final FileStatus fs : statusList) {
      if (fs.getModificationTime() < thresholdTime) {
        oldFiles.add(fs.getPath().getName());
      }
    }

    return oldFiles.toArray(new String[0]);
  }

  @Override
  public void deleteFile(final String pathString) throws Exception {
    ensureHdfs();

    try {
      this.hdfs.delete(new Path(pathString), true);
    } catch (final IOException e) {
      cleanUp();
    }
  }

  @Override
  public InputStream getFileInputStream(final String pathString) throws Exception {
    ensureHdfs();

    final Path path = new Path(pathString);

    return new BufferedInputStream(this.hdfs.open(path));
  }

  @Override
  public OutputStream getFileOutputStream(final String pathString) throws Exception {
    ensureHdfs();

    final Path path = new Path(pathString);

    return new BufferedOutputStream(this.hdfs.create(path, true));
  }

  @Override
  public void cleanUp() throws IOException {
    if (this.hdfs != null) {
      this.hdfs.close();
      this.hdfs = null;
    }
  }

  private void ensureHdfs() throws HadoopSecurityManagerException, IOException {
    if (this.hdfs == null) {
      if (this.securityManager == null) {
        this.hdfs = FileSystem.get(new Configuration());
      } else {
        this.hdfs = this.securityManager.getFSAsUser(this.username);
      }
    }
  }

  @Override
  protected void finalize() throws Throwable {
    cleanUp();
    super.finalize();
  }

  /**
   * Returns an array of the file statuses of the files/directories in the given
   * path if it is a directory and an empty array otherwise.
   */
  private FileStatus[] getFileStatusList(final String pathString)
      throws HadoopSecurityManagerException, IOException {
    ensureHdfs();

    final Path path = new Path(pathString);
    FileStatus pathStatus = null;
    try {
      pathStatus = this.hdfs.getFileStatus(path);
    } catch (final IOException e) {
      cleanUp();
    }

    if (pathStatus != null && pathStatus.isDir()) {
      return this.hdfs.listStatus(path);
    }

    return new FileStatus[0];
  }
}
