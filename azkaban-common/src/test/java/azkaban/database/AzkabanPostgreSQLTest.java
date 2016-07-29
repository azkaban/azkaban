/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.database;

import com.google.common.io.Resources;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import azkaban.utils.Props;

public class AzkabanPostgreSQLTest {
	
  @Ignore @Test
  public void testPostgreSQLAutoCreate() throws Exception {
    clearPostgreSQLTestDB();

    URL resourceUrl = Resources.getResource("conf/dbtestpostgresql");
    assertNotNull(resourceUrl);
    File resource = new File(resourceUrl.toURI());
    String confDir = resource.getParent();

    System.out.println("1.***Now testing check");
    AzkabanDatabaseUpdater.main(new String[] { "-c", confDir });

    System.out.println("2.***Now testing update");
    AzkabanDatabaseUpdater.main(new String[] { "-u", "-c", confDir });

    System.out.println("3.***Now testing check again");
    AzkabanDatabaseUpdater.main(new String[] { "-c", confDir });

    System.out.println("4.***Now testing update again");
    AzkabanDatabaseUpdater.main(new String[] { "-c", confDir, "-u" });

    System.out.println("5.***Now testing check again");
    AzkabanDatabaseUpdater.main(new String[] { "-c", confDir });
  }
  
  @Ignore @Test
  public void testPostgreSQLQuery() throws Exception {
    clearPostgreSQLTestDB();

	  URL resourceUrl = Resources.getResource("sql");
	  assertNotNull(resourceUrl);
	  String sqlScriptsDir = new File(resourceUrl.toURI()).getCanonicalPath();

    Props dbProps = getPostgreSQLProps(sqlScriptsDir);
    AzkabanDatabaseSetup setup = new AzkabanDatabaseSetup(dbProps);

    // First time will create the tables
    setup.loadTableInfo();
    setup.printUpgradePlan();
    setup.updateDatabase(true, true);
    assertTrue(setup.needsUpdating());

    // Second time will update some tables. This is only for testing purpose
    // and obviously we wouldn't set things up this way.
    setup.loadTableInfo();
    setup.printUpgradePlan();
    setup.updateDatabase(true, true);
    assertTrue(setup.needsUpdating());

    // Nothing to be done
    setup.loadTableInfo();
    setup.printUpgradePlan();
    assertFalse(setup.needsUpdating());
  }

private static Props getPostgreSQLProps(String sqlScriptsDir) {
    Props props = new Props();

    props.put("database.type", "postgresql");
    props.put("database.port", "5432");
    props.put("database.host", "localhost");
    props.put("database.database", "azkabanunittest");
    props.put("database.user", "postgres");
    props.put("database.sql.scripts.dir", sqlScriptsDir);
    props.put("database.password", "");
    props.put("database.numconnections", 10);

    return props;
  }

private static void clearPostgreSQLTestDB() throws SQLException {
    Props props = new Props();
    props.put("database.type", "postgresql");
    props.put("database.host", "localhost");
    props.put("database.port", "5432");
    props.put("database.database", "");
    props.put("database.user", "postgres");
    props.put("database.password", "");
    props.put("database.numconnections", 10);

    DataSource datasource = DataSourceUtils.getDataSource(props);
    QueryRunner runner = new QueryRunner(datasource);
    try {
      runner.update("drop database azkabanunittest");
    } catch (SQLException e) {
    }
    runner.update("create database azkabanunittest");
  }
}
