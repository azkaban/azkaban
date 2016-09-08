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

package azkaban.trigger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import org.joda.time.DateTime;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

import azkaban.database.DataSourceUtils;
import azkaban.executor.ExecutionOptions;
import azkaban.trigger.builtin.BasicTimeChecker;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.Props;
import azkaban.utils.Utils;

public class JdbcTriggerLoaderTest {

  private static boolean testDBExists = false;
  // @TODO remove this and turn into local host.
  private static final String host = "localhost";
  private static final int port = 3306;
  private static final String database = "azkaban2";
  private static final String user = "azkaban";
  private static final String password = "azkaban";
  private static final int numConnections = 10;

  private TriggerLoader loader;
  private CheckerTypeLoader checkerLoader;
  private ActionTypeLoader actionLoader;

  @Before
  public void setup() throws TriggerException {
    Props props = new Props();
    props.put("database.type", "mysql");

    props.put("mysql.host", host);
    props.put("mysql.port", port);
    props.put("mysql.user", user);
    props.put("mysql.database", database);
    props.put("mysql.password", password);
    props.put("mysql.numconnections", numConnections);

    loader = new JdbcTriggerLoader(props);
    checkerLoader = new CheckerTypeLoader();
    checkerLoader.init(new Props());
    checkerLoader.registerCheckerType(BasicTimeChecker.type,
        BasicTimeChecker.class);
    Condition.setCheckerLoader(checkerLoader);
    actionLoader = new ActionTypeLoader();
    actionLoader.init(new Props());

    actionLoader.registerActionType(ExecuteFlowAction.type,
        ExecuteFlowAction.class);
    Trigger.setActionTypeLoader(actionLoader);
    setupDB();
  }

  public void setupDB() {
    DataSource dataSource =
        DataSourceUtils.getMySQLDataSource(host, port, database, user,
            password, numConnections);
    testDBExists = true;

    Connection connection = null;
    try {
      connection = dataSource.getConnection();
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    CountHandler countHandler = new CountHandler();
    QueryRunner runner = new QueryRunner();
    try {
      runner.query(connection, "SELECT COUNT(1) FROM triggers", countHandler);
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    DbUtils.closeQuietly(connection);

    clearDB();
  }

  @After
  public void clearDB() {
    if (!testDBExists) {
      return;
    }

    DataSource dataSource =
        DataSourceUtils.getMySQLDataSource(host, port, database, user,
            password, numConnections);
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    QueryRunner runner = new QueryRunner();
    try {
      runner.update(connection, "DELETE FROM triggers");

    } catch (SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    DbUtils.closeQuietly(connection);
  }

  @Ignore @Test
  public void addTriggerTest() throws TriggerLoaderException {
    Trigger t1 = createTrigger("testProj1", "testFlow1", "source1");
    Trigger t2 = createTrigger("testProj2", "testFlow2", "source2");
    loader.addTrigger(t1);
    List<Trigger> ts = loader.loadTriggers();
    assertTrue(ts.size() == 1);

    Trigger t3 = ts.get(0);
    assertTrue(t3.getSource().equals("source1"));

    loader.addTrigger(t2);
    ts = loader.loadTriggers();
    assertTrue(ts.size() == 2);

    for (Trigger t : ts) {
      if (t.getTriggerId() == t2.getTriggerId()) {
        t.getSource().equals(t2.getSource());
      }
    }
  }

  @Ignore @Test
  public void removeTriggerTest() throws TriggerLoaderException {
    Trigger t1 = createTrigger("testProj1", "testFlow1", "source1");
    Trigger t2 = createTrigger("testProj2", "testFlow2", "source2");
    loader.addTrigger(t1);
    loader.addTrigger(t2);
    List<Trigger> ts = loader.loadTriggers();
    assertTrue(ts.size() == 2);
    loader.removeTrigger(t2);
    ts = loader.loadTriggers();
    assertTrue(ts.size() == 1);
    assertTrue(ts.get(0).getTriggerId() == t1.getTriggerId());
  }

  @Ignore @Test
  public void updateTriggerTest() throws TriggerLoaderException {
    Trigger t1 = createTrigger("testProj1", "testFlow1", "source1");
    t1.setResetOnExpire(true);
    loader.addTrigger(t1);
    List<Trigger> ts = loader.loadTriggers();
    assertTrue(ts.get(0).isResetOnExpire() == true);
    t1.setResetOnExpire(false);
    loader.updateTrigger(t1);
    ts = loader.loadTriggers();
    assertTrue(ts.get(0).isResetOnExpire() == false);
  }

  private Trigger createTrigger(String projName, String flowName, String source) {
    DateTime now = DateTime.now();
    ConditionChecker checker1 =
        new BasicTimeChecker("timeChecker1", now.getMillis(), now.getZone(),
            true, true, Utils.parsePeriodString("1h"), null);
    Map<String, ConditionChecker> checkers1 =
        new HashMap<String, ConditionChecker>();
    checkers1.put(checker1.getId(), checker1);
    String expr1 = checker1.getId() + ".eval()";
    Condition triggerCond = new Condition(checkers1, expr1);
    Condition expireCond = new Condition(checkers1, expr1);
    List<TriggerAction> actions = new ArrayList<TriggerAction>();
    TriggerAction action =
        new ExecuteFlowAction("executeAction", 1, projName, flowName,
            "azkaban", new ExecutionOptions(), null);
    actions.add(action);
    Trigger t =
        new Trigger(now.getMillis(), now.getMillis(), "azkaban", source,
            triggerCond, expireCond, actions);
    return t;
  }

  public static class CountHandler implements ResultSetHandler<Integer> {
    @Override
    public Integer handle(ResultSet rs) throws SQLException {
      int val = 0;
      while (rs.next()) {
        val++;
      }

      return val;
    }
  }

}
