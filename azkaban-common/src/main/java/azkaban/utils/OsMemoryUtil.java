package azkaban.utils;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for getting system memory information
 *
 * Note: This check is designed for Linux only.
 */
class OsMemoryUtil {

  private static final Logger logger = LoggerFactory.getLogger(OsMemoryUtil.class);

  // This file is used by Linux. It doesn't exist on Mac for example.
  private static final String MEM_INFO_FILE = "/proc/meminfo";

  static final ImmutableSet<String> MEM_KEYS = ImmutableSet
      .of("MemFree", "Buffers", "Cached", "SwapFree");

  static final ImmutableSet<String> PHYSICAL_MEM_KEYS = ImmutableSet.of("MemFree");

  /**
   * Includes OS cache and free swap.
   *
   * @return the total free memory size of the OS. 0 if there is an error or the OS doesn't support
   * this memory check.
   */
  long getOsTotalFreeMemorySize() {
    return getAggregatedFreeMemorySize(MEM_KEYS);
  }

  /**
   * @return the free physical memory size of the OS. 0 if there is an error or the OS doesn't
   * support this memory check.
   */
  long getOsFreePhysicalMemorySize() {
    return getAggregatedFreeMemorySize(PHYSICAL_MEM_KEYS);
  }

  private long getAggregatedFreeMemorySize(final Set<String> memKeysToCombine) {
    if (!Files.isRegularFile(Paths.get(MEM_INFO_FILE))) {
      // Mac doesn't support /proc/meminfo for example.
      return 0;
    }

    final List<String> lines;
    // The file /proc/meminfo is assumed to contain only ASCII characters.
    // The assumption is that the file is not too big. So it is simpler to read the whole file
    // into memory.
    try {
      lines = Files.readAllLines(Paths.get(MEM_INFO_FILE), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      final String errMsg = "Failed to open mem info file: " + MEM_INFO_FILE;
      logger.error(errMsg, e);
      return 0;
    }
    return getOsTotalFreeMemorySizeFromStrings(lines, memKeysToCombine);
  }

  /**
   * @param lines text lines from the procinfo file
   * @return the total size of free memory in kB. 0 if there is an error.
   */
  long getOsTotalFreeMemorySizeFromStrings(final List<String> lines,
      final Set<String> memKeysToCombine) {
    long totalFree = 0;
    int count = 0;

    for (final String line : lines) {
      for (final String keyName : memKeysToCombine) {
        if (line.startsWith(keyName)) {
          count++;
          final long size = parseMemoryLine(line);
          if (size == 0) {
            return 0;
          }
          totalFree += size;
        }
      }
    }

    final int length = memKeysToCombine.size();
    if (count != length) {
      final String errMsg = String
          .format("Expect %d keys in the meminfo file. Got %d. content: %s", length, count, lines);
      logger.error(errMsg);
      totalFree = 0;
    }
    return totalFree;
  }

  /**
   * Example file: $ cat /proc/meminfo MemTotal:       65894008 kB MemFree:        59400536 kB
   * Buffers:          409348 kB Cached:          4290236 kB SwapCached:            0 kB
   *
   * Make the method package private to make unit testing easier. Otherwise it can be made private.
   *
   * @param line the text for a memory usage statistics we are interested in
   * @return size of the memory. unit kB. 0 if there is an error.
   */
  long parseMemoryLine(final String line) {
    final int idx1 = line.indexOf(":");
    final int idx2 = line.lastIndexOf("kB");
    final String sizeString = line.substring(idx1 + 1, idx2 - 1).trim();
    try {
      return Long.parseLong(sizeString);
    } catch (final NumberFormatException e) {
      final String err = "Failed to parse the meminfo file. Line: " + line;
      logger.error(err);
      return 0;
    }
  }
}
