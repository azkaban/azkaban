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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface IStreamProvider {

  public void setUser(String user);

  public String[] getFileList(String pathString) throws Exception;

  /**
   * Returns a list of all files in a directory with a modification time less
   * than the specified time
   */
  public String[] getOldFiles(String pathString, long thresholdTime)
      throws Exception;


  /**
   * Deletes the file denoted by the specified path. If the file is a directory,
   * this method recursively deletes the files in the directory and the
   * directory itself.
   */
  public void deleteFile(String pathString) throws Exception;

  public InputStream getFileInputStream(String pathString) throws Exception;

  public OutputStream getFileOutputStream(String pathString) throws Exception;

  public void cleanUp() throws IOException;
}
