package azkaban.utils;

import javax.inject.Inject;
import org.slf4j.LoggerFactory;


/**
 * This class is used to maintain system memory information. Processes utilizing large amount of
 * memory should consult this class to see if the system has enough memory to proceed the
 * operation.
 *
 * Memory information is obtained from /proc/meminfo, so only Unix/Linux like system will support
 * this class.
 *
 * All the memory size used in this function is in KB.
 */
public class SystemMemoryInfo {

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SystemMemoryInfo.class);
  private static final long LOW_MEM_THRESHOLD = 3L * 1024L * 1024L; //3 GB
  private final OsMemoryUtil util;

  @Inject
  public SystemMemoryInfo(final OsMemoryUtil util) {
    this.util = util;
  }

  /**
   * @param xmx Xmx for the process
   * @return true if the system can satisfy the memory request
   *
   * Given Xmx value (in kb) used by java process, determine if system can satisfy the memory
   * request.
   */
  public boolean canSystemGrantMemory(final long xmx) {
    final long freeMemSize = this.util.getOsTotalFreeMemorySize();
    if (freeMemSize == 0) {
      // Fail open.
      // On the platforms that don't support the mem info file, the returned size will be 0.
      return true;
    }
    if (freeMemSize - xmx < LOW_MEM_THRESHOLD) {
      logger.info(String.format(
          "Free memory amount minus Xmx (%d - %d kb) is less than low mem threshold (%d kb), "
              + "memory request declined.",
          freeMemSize, xmx, LOW_MEM_THRESHOLD));
      return false;
    }
    return true;
  }
}
