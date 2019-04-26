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

import static azkaban.jobtype.hiveutils.azkaban.Utils.verifyProperty;
import static azkaban.jobtype.hiveutils.azkaban.hive.actions.Constants.DROP_ALL_PARTITIONS_AND_ADD_LATEST;

import azkaban.jobtype.hiveutils.HiveQueryExecutionException;
import azkaban.jobtype.hiveutils.HiveQueryExecutor;
import azkaban.jobtype.hiveutils.azkaban.HiveAction;
import azkaban.jobtype.hiveutils.azkaban.HiveViaAzkabanException;
import azkaban.jobtype.hiveutils.util.AzkHiveAction;
import azkaban.jobtype.hiveutils.util.AzkabanJobPropertyDescription;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Drop all the existing partitions in specified table and then add the
 * latest/greatest/highest directory in the specified subdirectory and add that
 * as the only partition in the table. This action is suitable for tables with
 * full replacement policies where we only want one partition.</p>
 *
 * For example, if we have a table foo with derived_date partitions that
 * correspond to directories:</p> /foo/2012-01-01</p> /2012-01-02</p>
 * /2012-01-03</p> And currently only have derived_date="2012-01-02" registered,
 * we want to drop that partition and add "2012-01-03" as the only
 * partition.</p> </p> This action makes several assumptions:
 * <ul>
 * <li>The databases are named the same as the directories in tableLocations, ie
 * /derived/member corresponds to some database with a table named member</li>
 * <li>The table is has only a single partition column</li>
 * <li>The partition name corresponds to directories within the specified
 * tableLocations</li>
 * <li>The partition values are the same name as the directories within the
 * specified tableLocations</li>
 * <li>The partition directories are named in ascending order such that a sort
 * will give us the one to add and we will drop all but that one</li>
 * </ul>
 */
@AzkHiveAction(DROP_ALL_PARTITIONS_AND_ADD_LATEST)
public class DropAllPartitionsAddLatest implements HiveAction {

  private final static Logger logger =
      LoggerFactory.getLogger("com.linkedin.hive.azkaban.hive.actions.DropAllPartitionsAddLatest");

  public static final String DROP_AND_ADD = DROP_ALL_PARTITIONS_AND_ADD_LATEST;

  @AzkabanJobPropertyDescription("Comma-separated list of tables to drop/add partitions to.  All tables must be within the same database")
  public static final String HIVE_TABLE = "hive.tables";
  @AzkabanJobPropertyDescription("Database to drop/add partitions within")
  public static final String HIVE_DATABASE = "hive.database";
  @AzkabanJobPropertyDescription("Name of partition of drop/add from table, eg. datepartition")
  public static final String HIVE_PARTITION = "hive.partition";
  @AzkabanJobPropertyDescription("Directory on hdfs where external table resides, eg /data/derived/. Tables should be in this directory.")
  public static final String HIVE_TABLES_LOCATION = "hive.tables.location";

  private final String database;
  private final String[] tables;
  private final String partition;
  private HiveQueryExecutor hqe;
  private String tableLocations;

  public DropAllPartitionsAddLatest(Properties p, HiveQueryExecutor hqe)
      throws HiveViaAzkabanException {
    // The goal here is to get to a fluent API ala
    // LinkedInHive.get("magic")
    //     .forDatabase("u_jhoman")
    //     .forTable("zoiks")
    //     .dropPartition("date-stamp","2012-01-01")
    //     .addPartition("date-stamp","2012-01-02", "/some/path").go();
    this.database = verifyProperty(p, HIVE_DATABASE);
    this.tables = verifyProperty(p, HIVE_TABLE).split(",");
    this.partition = verifyProperty(p, HIVE_PARTITION);
    this.tableLocations = verifyProperty(p, HIVE_TABLES_LOCATION);
    this.hqe = hqe;
  }

  @Override
  public void execute() throws HiveViaAzkabanException {
    ArrayList<HQL> hql = new ArrayList<HQL>();
    hql.add(new UseDatabaseHQL(database));

    Configuration conf = new Configuration();
    try {
      FileSystem fs = FileSystem.get(conf);

      for (String table : tables) {
        logger.info("Determining HQL commands for table " + table);
        hql.addAll(addAndDrop(fs, tableLocations, table));
      }
      fs.close();
    } catch (IOException e) {
      throw new HiveViaAzkabanException(
          "Exception fetching the directories/partitions from HDFS", e);
    }

    StringBuffer query = new StringBuffer();
    for (HQL q : hql) {
      query.append(q.toHQL()).append("\n");
    }

    System.out.println("Query to execute:\n" + query.toString());
    try {
      hqe.executeQuery(query.toString());
    } catch (HiveQueryExecutionException e) {
      throw new HiveViaAzkabanException("Problem executing query ["
          + query.toString() + "] on Hive", e);
    }

  }

  private ArrayList<HQL> addAndDrop(FileSystem fs, String basepath, String table)
      throws IOException, HiveViaAzkabanException {
    ArrayList<HQL> toDropAndAdd = new ArrayList<HQL>();
    ArrayList<String> directories = null;

    directories = Utils.fetchDirectories(fs, basepath + "/" + table, false);

    if (directories.size() == 0) {
      throw new HiveViaAzkabanException(
          "No directories to remove or add found in " + tableLocations);
    }

    Collections.sort(directories);

    String toAdd = directories.remove(directories.size() - 1);

    logger.info("For table " + table + ", going to add " + toAdd
        + " and attempt to drop " + directories.size() + " others");
    for (String directory : directories) {
      toDropAndAdd.add(new DropPartitionHQL(table, partition, directory, true));
    }

    toDropAndAdd.add(new AddExternalPartitionHQL(table, partition, toAdd,
        toAdd, true));
    return toDropAndAdd;
  }
}
