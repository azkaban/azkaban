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

package azkaban.scheduler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import azkaban.utils.JSONUtils;

/**
 * TODO: This needs to be fleshed out and made into a proper singleton.
 */
public class ScheduleStatisticManager {
  public static final int STAT_NUMBERS = 10;

  private static HashMap<Integer, Object> cacheLock =
      new HashMap<Integer, Object>();
  private static File cacheDirectory;

  public static void invalidateCache(int scheduleId, File cacheDir) {
    setCacheFolder(cacheDir);
    // This should be silent and not fail
    try {
      Object lock = getLock(scheduleId);
      synchronized (lock) {
        getCacheFile(scheduleId).delete();
      }
      unLock(scheduleId);
    } catch (Exception e) {
    }
  }

  public static void saveCache(int scheduleId, Map<String, Object> data) {
    Object lock = getLock(scheduleId);
    try {
      synchronized (lock) {
        File cache = getCacheFile(scheduleId);
        cache.createNewFile();
        OutputStream output = new FileOutputStream(cache);
        try {
          JSONUtils.toJSON(data, output, false);
        } finally {
          output.close();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    unLock(scheduleId);
  }

  public static Map<String, Object> loadCache(int scheduleId) {
    Object lock = getLock(scheduleId);
    try {
      synchronized (lock) {
        File cache = getCacheFile(scheduleId);
        if (cache.exists() && cache.isFile()) {
          Object dataObj = JSONUtils.parseJSONFromFile(cache);
          if (dataObj instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            Map<String, Object> data = (Map<String, Object>) dataObj;
            return data;
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    unLock(scheduleId);
    return null;
  }

  public static File getCacheDirectory() {
    return cacheDirectory;
  }

  private static File getCacheFile(int scheduleId) {
    cacheDirectory.mkdirs();
    File file = new File(cacheDirectory, scheduleId + ".cache");
    return file;
  }

  private static Object getLock(int scheduleId) {
    Object lock = null;
    synchronized (cacheLock) {
      lock = cacheLock.get(scheduleId);
      if (lock == null) {
        lock = new Object();
        cacheLock.put(scheduleId, lock);
      }
    }

    return lock;
  }

  private static void unLock(int scheduleId) {
    synchronized (cacheLock) {
      cacheLock.remove(scheduleId);
    }
  }

  public static void setCacheFolder(File cacheDir) {
    if (cacheDirectory == null) {
      cacheDirectory = new File(cacheDir, "schedule-statistics");
    }
  }
}

