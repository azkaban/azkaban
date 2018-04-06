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

package azkaban.jobtype.hiveutils.azkaban;

import java.util.Properties;

import azkaban.jobtype.hiveutils.HiveQueryExecutor;
import azkaban.jobtype.hiveutils.HiveUtils;
import azkaban.jobtype.hiveutils.azkaban.hive.actions.DropAllPartitionsAddLatest;
import azkaban.jobtype.hiveutils.azkaban.hive.actions.ExecuteHiveQuery;
import azkaban.jobtype.hiveutils.azkaban.hive.actions.UpdateTableLocationToLatest;

/**
 * Simple Java driver class to execute a Hive query provided via the Properties
 * file. The query can be specified via: * hive.query = a single-line query that
 * will be fed to Hive * hive.query.nn = a two-digit padded series of lines that
 * will be joined and fed to to Hive as one big query
 */
public class HiveViaAzkaban {
  final private static String AZK_HIVE_ACTION = "azk.hive.action";
  public static final String EXECUTE_QUERY = "execute.query";

  private Properties p;

  public HiveViaAzkaban(String jobName, Properties p) {
    this.p = p;
  }

  public void run() throws HiveViaAzkabanException {
    if (p == null) {
      throw new HiveViaAzkabanException("Properties is null.  Can't continue");
    }

    if (!p.containsKey(AZK_HIVE_ACTION)) {
      throw new HiveViaAzkabanException("Must specify a " + AZK_HIVE_ACTION
          + " key and value.");
    }
    HiveQueryExecutor hqe = HiveUtils.getHiveQueryExecutor();
    HiveAction action = null;
    String hive_action = p.getProperty(AZK_HIVE_ACTION);
    // TODO: Factory time
    if (hive_action.equals(EXECUTE_QUERY)) {
      action = new ExecuteHiveQuery(p, hqe);
    } else if (hive_action.equals(DropAllPartitionsAddLatest.DROP_AND_ADD)) {
      action = new DropAllPartitionsAddLatest(p, hqe);
    } else if (hive_action
        .equals(UpdateTableLocationToLatest.UPDATE_TABLE_LOCATION_TO_LATEST)) {
      action = new UpdateTableLocationToLatest(p, hqe);
    } else {
      throw new HiveViaAzkabanException("Unknown value (" + hive_action
          + ") for value " + AZK_HIVE_ACTION);
    }

    action.execute();
  }
}
