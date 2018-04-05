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

package azkaban.jobtype.hiveutils;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.log4j.Logger;

/**
 * Grab bag of utilities for working with Hive. End users should obtain
 * instances of the provided interfaces from these methods.
 */
public class HiveUtils {
  private final static Logger LOG =
      Logger.getLogger("com.linkedin.hive.HiveUtils");

  private HiveUtils() {
  }

  public static HiveQueryExecutor getHiveQueryExecutor() {
    HiveQueryExecutorModule hqem = new HiveQueryExecutorModule();
    try {
      return new RealHiveQueryExecutor(hqem.provideHiveConf(),
          hqem.provideCliSessionState(), new CliDriver());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Normally hive.aux.jars.path is expanded from just being a path to the full
   * list of files in the directory by the hive shell script. Since we normally
   * won't be running from the script, it's up to us to do that work here. We
   * use a heuristic that if there is no occurrence of ".jar" in the original,
   * it needs expansion. Otherwise it's already been done for us.
   *
   * Also, surround the files with uri niceities.
   */
  static String expandHiveAuxJarsPath(String original) throws IOException {
    if (original == null || original.contains(".jar"))
      return original;

    File[] files = new File(original).listFiles();

    if (files == null || files.length == 0) {
      LOG.info("No files in to expand in aux jar path. Returning original parameter");
      return original;
    }

    return filesToURIString(files);

  }

  static String filesToURIString(File[] files) throws IOException {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < files.length; i++) {
      sb.append("file:///").append(files[i].getCanonicalPath());
      if (i != files.length - 1)
        sb.append(",");
    }

    return sb.toString();
  }
}
