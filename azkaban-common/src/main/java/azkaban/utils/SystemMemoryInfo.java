package azkaban.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.LoggerFactory;


/**
 * This class is used to maintain system memory information. Processes utilizing
 * large amount of memory should consult this class to see if the system has enough
 * memory to proceed the operation.
 *
 * Memory information is obtained from /proc/meminfo, so only Unix/Linux like system
 * will support this class.
 *
 * All the memory size used in this function is in KB
 */
public class SystemMemoryInfo {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SystemMemoryInfo.class);

  private static boolean memCheckEnabled;
  private static final long LOW_MEM_THRESHOLD = 3L * 1024L * 1024L; //3 GB
  // In case there is a problem reading the meminfo file, we want to "fail open".
  private static long freeMemAmount = LOW_MEM_THRESHOLD * 100;

  private static ScheduledExecutorService scheduledExecutorService;

  //todo HappyRay: switch to Guice
  private static OsMemoryUtil util = new OsMemoryUtil();

  @SuppressWarnings("FutureReturnValueIgnored")
  // see http://errorprone.info/bugpattern/FutureReturnValueIgnored
  // There is no need to check the returned future from scheduledExecutorService
  // since we don't need to get a return value.
  public static void init(int memCheckInterval) {
    memCheckEnabled = util.doesMemInfoFileExist();
    if (memCheckEnabled) {
      //schedule a thread to read it
      logger.info(String.format("Scheduled thread to read memory info every %d seconds", memCheckInterval));
      scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
      /*
      According to java.util.concurrent.Executors.newSingleThreadScheduledExecutor()
      * (Note however that if this single
     * thread terminates due to a failure during execution prior to
     * shutdown, a new one will take its place if needed to execute
     * subsequent tasks.)
     * We don't have to worry about the update thread dying due to an uncaught exception.
       */
      scheduledExecutorService.scheduleAtFixedRate(SystemMemoryInfo::getFreeMemorySize, 0, memCheckInterval,
          TimeUnit.SECONDS);
    } else {
      logger.info(String.format("Cannot find %s, memory check will be disabled", OsMemoryUtil.MEM_INFO_FILE));
    }
  }

  /**
   * @param xms Xms for the process
   * @param xmx Xmx for the process
   * @return System can satisfy the memory request or not
   *
   * Given Xms/Xmx values (in kb) used by java process, determine if system can
   * satisfy the memory request
   */
  public synchronized static boolean canSystemGrantMemory(long xms, long xmx, long freeMemDecrAmt) {
    if (!memCheckEnabled) {
      return true;
    }

    //too small amount of memory left, reject
    if (freeMemAmount < LOW_MEM_THRESHOLD) {
      logger.info(
          String.format("Free memory amount (%d kb) is less than low mem threshold (%d kb),  memory request declined.",
              freeMemAmount, LOW_MEM_THRESHOLD));
      return false;
    }

    //let's get newest mem info
    if (freeMemAmount >= LOW_MEM_THRESHOLD && freeMemAmount < 2 * LOW_MEM_THRESHOLD) {
      logger.info(String.format(
          "Free memory amount (%d kb) is less than 2x low mem threshold (%d kb). Update the free memory amount",
          freeMemAmount, LOW_MEM_THRESHOLD));
      getFreeMemorySize();
    }

    //too small amount of memory left, reject
    if (freeMemAmount < LOW_MEM_THRESHOLD) {
      logger.info(
          String.format("Free memory amount (%d kb) is less than low mem threshold (%d kb),  memory request declined.",
              freeMemAmount, LOW_MEM_THRESHOLD));
      return false;
    }

    if (freeMemAmount - xmx < LOW_MEM_THRESHOLD) {
      logger.info(String.format(
          "Free memory amount minus xmx (%d - %d kb) is less than low mem threshold (%d kb),  memory request declined.",
          freeMemAmount, xmx, LOW_MEM_THRESHOLD));
      return false;
    }

    if (freeMemDecrAmt > 0) {
      freeMemAmount -= freeMemDecrAmt;
      logger.info(
          String.format("Memory (%d kb) granted. Current free memory amount is %d kb", freeMemDecrAmt, freeMemAmount));
    } else {
      freeMemAmount -= xms;
      logger.info(String.format("Memory (%d kb) granted. Current free memory amount is %d kb", xms, freeMemAmount));
    }

    return true;
  }

  private synchronized static void updateFreeMemAmount(long size) {
    freeMemAmount = size;
  }

  private static void getFreeMemorySize() {
    long freeMemorySize = util.getOsTotalFreeMemorySize();
    if (freeMemorySize > 0) {
      updateFreeMemAmount(freeMemorySize);
    }
  }

  public static void shutdown() {
    logger.warn("Shutting down SystemMemoryInfo...");
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdown();
    }
  }
}
