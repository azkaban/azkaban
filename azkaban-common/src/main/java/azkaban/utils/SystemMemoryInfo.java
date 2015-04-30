package azkaban.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * @author wkang
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
  private static final Logger logger = Logger.getLogger(SystemMemoryInfo.class);

  private static String MEMINFO_FILE = "/proc/meminfo"; 
  private static boolean memCheckEnabled;
  private static long freeMemAmount = 0;
  private static final long LOW_MEM_THRESHOLD = 3L*1024L*1024L; //3 GB

  private static ScheduledExecutorService scheduledExecutorService;

  public static void init(int memCheckInterval) {
    File f = new File(MEMINFO_FILE);
    memCheckEnabled = f.exists() && !f.isDirectory();
    if (memCheckEnabled) {
      //initial reading of the mem info
      readMemoryInfoFile();

      //schedule a thread to read it
      logger.info(String.format("Scheduled thread to read /proc/meminfo every %d seconds", memCheckInterval));
      scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
      scheduledExecutorService.scheduleAtFixedRate(new MemoryInfoReader(), 0, memCheckInterval, TimeUnit.SECONDS);
    } else {
      logger.info("Cannot find /proc/meminfo, memory check will be disabled");
    }
  }

  /**
   * @param xms
   * @param xmx
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
      logger.info(String.format("Free memory amount (%d kb) is less than low mem threshold (%d kb),  memory request declined.",
              freeMemAmount, LOW_MEM_THRESHOLD));
      return false;
    }

    //let's get newest mem info
    if (freeMemAmount >= LOW_MEM_THRESHOLD && freeMemAmount < 2 * LOW_MEM_THRESHOLD) {
      logger.info(String.format("Free memory amount (%d kb) is less than 2x low mem threshold (%d kb),  re-read /proc/meminfo",
              freeMemAmount, LOW_MEM_THRESHOLD));
      readMemoryInfoFile();
    }

    //too small amount of memory left, reject
    if (freeMemAmount < LOW_MEM_THRESHOLD) {
      logger.info(String.format("Free memory amount (%d kb) is less than low mem threshold (%d kb),  memory request declined.",
              freeMemAmount, LOW_MEM_THRESHOLD));
      return false;
    }

    if (freeMemAmount - xmx < LOW_MEM_THRESHOLD) {
      logger.info(String.format("Free memory amount minus xmx (%d - %d kb) is less than low mem threshold (%d kb),  memory request declined.",
              freeMemAmount, xmx, LOW_MEM_THRESHOLD));
      return false;
    }

    if (freeMemDecrAmt > 0) {
      freeMemAmount -= freeMemDecrAmt;
      logger.info(String.format("Memory (%d kb) granted. Current free memory amount is %d kb", freeMemDecrAmt, freeMemAmount));
    } else {
      freeMemAmount -= xms;
      logger.info(String.format("Memory (%d kb) granted. Current free memory amount is %d kb", xms, freeMemAmount));
    }
    
    return true;
  }

  private synchronized static void updateFreeMemAmount(long size) {
    freeMemAmount = size;
  }

  private static void readMemoryInfoFile() {
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(MEMINFO_FILE));

      long sizeMemFree = 0;
      long sizeBuffers = 0;
      long sizeCached = 0;
      long sizeSwapCached = 0;
      int count = 0;
      String line = br.readLine();
      while (line != null) {
        if (line.startsWith("MemFree:") || line.startsWith("Buffers:")
            || line.startsWith("Cached") || line.startsWith("SwapCached")) {
          int idx1 = line.indexOf(":");
          int idx2 = line.lastIndexOf("kB");
          String strSize = line.substring(idx1+1, idx2-1).trim();
          
          if (line.startsWith("MemFree:")) {
            sizeMemFree = Long.parseLong(strSize);
          } else if (line.startsWith("Buffers:")) {
            sizeBuffers = Long.parseLong(strSize);
          } else if (line.startsWith("Cached:")) {
            sizeCached = Long.parseLong(strSize);
          } else if (line.startsWith("SwapCached:")) {
            sizeSwapCached = Long.parseLong(strSize);
          }

          //all lines read
          if (++count == 4) {
            break;
          }
        }
        line = br.readLine();
      }

      if (count < 4) {
        logger.error("Error: less than 4 rows read from /proc/meminfo for free memory information");
      }

      long sizeTotal = sizeMemFree + sizeBuffers + sizeCached + sizeSwapCached;

      logger.info(String.format("Current system free memory is %d kb (MemFree %d, Buffers %d, Cached %d, SwapCached %d)",
              sizeTotal, sizeMemFree, sizeBuffers, sizeCached, sizeSwapCached));

      if (sizeTotal > 0) {
        updateFreeMemAmount(sizeTotal);
      }
    } catch (IOException e) {
      logger.error("Exception in reading memory info file", e);
    } finally {
      try {
        if (br != null) {
          br.close();
        }
      } catch (IOException e) {
        logger.error("Exception in closing the buffered reader", e);
      }
    }
  }

  static class MemoryInfoReader implements Runnable {
    @Override
    public void run() {
      try {
        readMemoryInfoFile();
      } catch (Throwable t) {
        logger.error("error calling readMemoryInfoFile", t);
      }
    }
  }
}