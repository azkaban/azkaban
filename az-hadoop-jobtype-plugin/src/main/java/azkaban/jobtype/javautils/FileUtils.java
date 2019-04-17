package azkaban.jobtype.javautils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.log4j.Logger;


public class FileUtils {

  private static Logger logger = Logger.getLogger(FileUtils.class);

  /**
   * Delete file or directory.
   * (Apache FileUtils.deleteDirectory has a bug and is not working.)
   */
  public static void deleteFileOrDirectory(File file) throws IOException {
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

  public static boolean tryDeleteFileOrDirectory(File file) {
    try {
      deleteFileOrDirectory(file);
      return true;
    } catch (Exception e) {
      logger.warn("Failed to delete " + file.getAbsolutePath(), e);
      return false;
    }
  }

  /**
   * Find files while input can use wildcard * or ?
   *
   * @param filesStr File path(s) delimited by delimiter
   * @param delimiter Separator of file paths.
   * @return List of absolute path of files
   */
  public static Collection<String> listFiles(String filesStr, String delimiter) {
    ValidationUtils.validateNotEmpty(filesStr, "fileStr");

    List<String> files = new ArrayList<String>();
    for (String s : filesStr.split(delimiter)) {
      File f = new File(s);
      if (!f.getName().contains("*") && !f.getName().contains("?")) {
        files.add(f.getAbsolutePath());
        continue;
      }

      FileFilter fileFilter = new AndFileFilter(new WildcardFileFilter(f.getName()),
          FileFileFilter.FILE);
      File parent = f.getParentFile() == null ? f : f.getParentFile();
      File[] filteredFiles = parent.listFiles(fileFilter);
      if (filteredFiles == null) {
        continue;
      }

      for (File file : filteredFiles) {
        files.add(file.getAbsolutePath());
      }
    }
    return files;
  }
}
