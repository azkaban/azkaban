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
package azkaban.jobtype.javautils;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utilities Function of File Related Operations
 */
public class FileUtils {

  private static Logger LOG = LoggerFactory.getLogger(FileUtils.class);

  /**
   * Delete file or directory.
   * (Apache FileUtils.deleteDirectory has a bug and is not working.)
   */
  public static void deleteFileOrDirectory(File file) {
    if (!file.isDirectory()) {
      file.delete();
      return;
    }

    if (file.list().length == 0) { //Nothing under directory. Just delete it.
      file.delete();
      return;
    }

    for (String temp : file.list()) { //Delete files or directory under current directory.
      File fileDelete = new File(file, temp);
      deleteFileOrDirectory(fileDelete);
    }
    //Now there is nothing under directory, delete it.
    deleteFileOrDirectory(file);
  }

  /**
   * Try to delete File or Directory
   *
   * @param file file object
   * @return success delete or not
   */
  public static boolean tryDeleteFileOrDirectory(File file) {
    try {
      deleteFileOrDirectory(file);
      return true;
    } catch (Exception e) {
      LOG.warn("Failed to delete file. file = " + file.getAbsolutePath(), e);
      return false;
    }
  }

  /**
   * Find files while input can use wildcard * or ?
   *
   * @param filesString File path(s) delimited by delimiter
   * @param delimiter Separator of file paths.
   * @return List of absolute path of files
   */
  public static Collection<String> listFiles(String filesString, String delimiter) {
    ValidationUtils.validateNotEmpty(filesString, "fileStr");

    List<String> files = new ArrayList<String>();
    for (String fileString : filesString.split(delimiter)) {
      File file = new File(fileString);
      if (!file.getName().contains("*") && !file.getName().contains("?")) {
        files.add(file.getAbsolutePath());
        continue;
      }

      FileFilter fileFilter = new AndFileFilter(new WildcardFileFilter(file.getName()),
          FileFileFilter.FILE);
      File parent = file.getParentFile() == null ? file : file.getParentFile();
      File[] filteredFiles = parent.listFiles(fileFilter);
      if (filteredFiles == null) {
        continue;
      }

      for (File filteredFile : filteredFiles) {
        files.add(filteredFile.getAbsolutePath());
      }
    }
    return files;
  }
}
