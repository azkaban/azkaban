package azkaban.utils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for getting system memory information
 *
 * Note:
 * This check is designed for Linux only.
 * Make sure to call {@link #doesMemInfoFileExist()} first before attempting to get memory information.
 */
class OsMemoryUtil {
  private static final Logger logger = LoggerFactory.getLogger(SystemMemoryInfo.class);

  // This file is used by Linux. It doesn't exist on Mac for example.
  static final String MEM_INFO_FILE = "/proc/meminfo";

  private final String[] MEM_KEYS;

  OsMemoryUtil() {
    MEM_KEYS = new String[]{"MemFree", "Buffers", "Cached", "SwapFree"};
  }

  /**
   *
   * @return true if the meminfo file exists.
   */
  boolean doesMemInfoFileExist() {
    File f = new File(MEM_INFO_FILE);
    return f.exists() && !f.isDirectory();
  }

  /**
   * Includes OS cache and free swap.
   * @return the total free memory size of the OS. 0 if there is an error.
   */
  long getOsTotalFreeMemorySize() {
    List<String> lines;
    // The file /proc/meminfo seems to contain only ASCII characters.
    // The assumption is that the file is not too big. So it is simpler to read the whole file into memory.
    try {
      lines = Files.readAllLines(Paths.get(MEM_INFO_FILE), StandardCharsets.UTF_8);
    } catch (IOException e) {
      String errMsg = "Failed to open mem info file: " + MEM_INFO_FILE;
      logger.warn(errMsg, e);
      return 0;
    }
    return getOsTotalFreeMemorySizeFromStrings(lines);
  }

  /**
   *
   * @param lines text lines from the procinfo file
   * @return the total size of free memory in kB. 0 if there is an error.
   */
  long getOsTotalFreeMemorySizeFromStrings(List<String> lines) {
    long totalFree = 0;
    int count = 0;

    for (String line : lines) {
      for (String keyName : MEM_KEYS) {
        if (line.startsWith(keyName)) {
          count++;
          long size = parseMemoryLine(line);
          if (size == 0) {
            return 0;
          }
          totalFree += size;
        }
      }
    }

    int length = MEM_KEYS.length;
    if (count != length) {
      String errMsg = String.format("Expect %d keys in the meminfo file. Got %d. content: %s", length, count, lines);
      logger.warn(errMsg);
      totalFree = 0;
    }
    return totalFree;
  }

  /**
   * Example file:
   * $ cat /proc/meminfo
   *   MemTotal:       65894008 kB
   *   MemFree:        59400536 kB
   *   Buffers:          409348 kB
   *   Cached:          4290236 kB
   *   SwapCached:            0 kB
   *
   * Make the method package private to make unit testing easier.
   * Otherwise it can be made private.

   * @param line the text for a memory usage statistics we are interested in
   * @return size of the memory. unit kB. 0 if there is an error.
   */
  long parseMemoryLine(String line) {
    int idx1 = line.indexOf(":");
    int idx2 = line.lastIndexOf("kB");
    String sizeString = line.substring(idx1 + 1, idx2 - 1).trim();
    try {
      return Long.parseLong(sizeString);
    } catch (NumberFormatException e) {
      String err = "Failed to parse the meminfo file. Line: " + line;
      logger.warn(err);
      return 0;
    }
  }
}
