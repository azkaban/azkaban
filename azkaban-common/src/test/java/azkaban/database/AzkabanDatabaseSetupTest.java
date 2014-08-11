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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URISyntaxException;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import azkaban.utils.Props;

public class AzkabanDatabaseSetupTest {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static String sqlScriptsDir;

  @BeforeClass
  public static void setupDB() throws IOException, URISyntaxException {
    URL resourceUrl = Resources.getResource("sql");
    assertNotNull(resourceUrl);
    sqlScriptsDir = new File(resourceUrl.toURI()).getCanonicalPath();
  }

  @AfterClass
  public static void teardownDB() {
  }

  @Ignore @Test
  public void testH2Query() throws Exception {
    File dbDir = temp.newFolder("h2dbtest");
    Props h2Props = getH2Props(dbDir.getCanonicalPath(), sqlScriptsDir);
    AzkabanDatabaseSetup setup = new AzkabanDatabaseSetup(h2Props);

    // First time will create the tables
    setup.loadTableInfo();
    setup.printUpgradePlan();
    setup.updateDatabase(true, true);
    assertTrue(setup.needsUpdating());

    // Second time will update some tables. This is only for testing purpose and
    // obviously we wouldn't set things up this way.
    setup.loadTableInfo();
    setup.printUpgradePlan();
    setup.updateDatabase(true, true);
    assertTrue(setup.needsUpdating());

    // Nothing to be done
    setup.loadTableInfo();
    setup.printUpgradePlan();
    assertFalse(setup.needsUpdating());
  }

  @Ignore @Test
  public void testMySQLQuery() throws Exception {
    clearMySQLTestDB();
    Props mysqlProps = getMySQLProps(sqlScriptsDir);
    AzkabanDatabaseSetup setup = new AzkabanDatabaseSetup(mysqlProps);

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

  private static Props getH2Props(String dbDir, String sqlScriptsDir) {
    Props props = new Props();
    props.put("database.type", "h2");
    props.put("h2.path", dbDir);
    props.put("database.sql.scripts.dir", sqlScriptsDir);
    return props;
  }

  private static Props getMySQLProps(String sqlScriptsDir) {
    Props props = new Props();

    props.put("database.type", "mysql");
    props.put("mysql.port", "3306");
    props.put("mysql.host", "localhost");
    props.put("mysql.database", "azkabanunittest");
    props.put("mysql.user", "root");
    props.put("database.sql.scripts.dir", sqlScriptsDir);
    props.put("mysql.password", "");
    props.put("mysql.numconnections", 10);

    return props;
  }

  private static void clearMySQLTestDB() throws SQLException {
    Props props = new Props();
    props.put("database.type", "mysql");
    props.put("mysql.host", "localhost");
    props.put("mysql.port", "3306");
    props.put("mysql.database", "");
    props.put("mysql.user", "root");
    props.put("mysql.password", "");
    props.put("mysql.numconnections", 10);

    DataSource datasource = DataSourceUtils.getDataSource(props);
    QueryRunner runner = new QueryRunner(datasource);
    try {
      runner.update("drop database azkabanunittest");
    } catch (SQLException e) {
    }
    runner.update("create database azkabanunittest");
  }
}
