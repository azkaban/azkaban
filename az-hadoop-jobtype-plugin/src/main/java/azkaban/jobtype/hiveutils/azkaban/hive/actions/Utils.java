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

package azkaban.jobtype.hiveutils.azkaban.hive.actions;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import azkaban.jobtype.hiveutils.azkaban.HiveViaAzkabanException;

import java.io.IOException;
import java.util.ArrayList;

class Utils {
  private final static Logger LOG = Logger.getLogger(Utils.class);

  static ArrayList<String> fetchDirectories(FileSystem fs, String location,
      boolean returnFullPath) throws IOException, HiveViaAzkabanException {
    LOG.info("Fetching directories in " + location);
    Path p = new Path(location);
    FileStatus[] statuses = fs.listStatus(p);

    if (statuses == null || statuses.length == 0) {
      throw new HiveViaAzkabanException("Couldn't find any directories in "
          + location);
    }

    ArrayList<String> files = new ArrayList<String>(statuses.length);
    for (FileStatus status : statuses) {
      if (!status.isDir())
        continue;
      if (status.getPath().getName().startsWith("."))
        continue;

      files.add(returnFullPath ? status.getPath().toString() : status.getPath()
          .getName());
    }
    return files;
  }
}
