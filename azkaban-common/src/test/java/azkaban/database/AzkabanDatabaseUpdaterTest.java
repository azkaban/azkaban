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
import java.net.URL;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

import azkaban.utils.Props;

public class AzkabanDatabaseUpdaterTest {
  @Ignore @Test
  public void testMySQLAutoCreate() throws Exception {
    clearMySQLTestDb();

    URL resourceUrl = Resources.getResource("conf/dbtestmysql");
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

  @Test
  public void testH2AutoCreate() throws Exception {
    URL resourceUrl = Resources.getResource("conf/dbtesth2");
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

  private static void clearMySQLTestDb() throws SQLException {
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
