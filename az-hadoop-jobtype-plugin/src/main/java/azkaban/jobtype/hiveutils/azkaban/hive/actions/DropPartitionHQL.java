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

class DropPartitionHQL implements HQL {
  private final String table;
  private final String partition;
  private final String value;
  private final boolean ifExists;

  DropPartitionHQL(String table, String partition, String value,
      boolean ifExists) {
    // @TODO: Null checks
    this.table = table;
    this.partition = partition;
    this.value = value;
    this.ifExists = ifExists;
  }

  @Override
  public String toHQL() {
    String exists = ifExists ? "IF EXISTS " : "";

    return "ALTER TABLE " + table + " DROP " + exists + "PARTITION ("
        + partition + "='" + value + "');";
  }
}
