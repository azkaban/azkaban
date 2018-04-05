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

class AlterTableLocationQL implements HQL {
  private final String table;
  private final String newLocation;

  public AlterTableLocationQL(String table, String newLocation) {
    // @TODO: Null checks
    this.table = table;
    this.newLocation = newLocation;
  }

  @Override
  public String toHQL() {
    return "ALTER TABLE " + table + " SET LOCATION '" + newLocation + "';";
  }
}
