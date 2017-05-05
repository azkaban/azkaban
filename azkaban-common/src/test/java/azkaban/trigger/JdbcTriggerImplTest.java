/*
 * Copyright 2017 LinkedIn Corp.
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

import azkaban.database.AzkabanConnectionPoolTest;
import azkaban.database.AzkabanDataSource;
import azkaban.database.AzkabanDatabaseSetup;
import azkaban.db.DatabaseOperator;
import azkaban.db.DatabaseOperatorImpl;
import azkaban.executor.ExecutionOptions;
import azkaban.trigger.builtin.BasicTimeChecker;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import com.google.common.io.Resources;
import java.io.File;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbutils.QueryRunner;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;


public class JdbcTriggerImplTest {

  TriggerLoader loader;
  DatabaseOperator dbOperator;
  public static AzkabanDataSource dataSource = new AzkabanConnectionPoolTest.EmbeddedH2BasicDataSource();

  @BeforeClass
  public static void prepare() throws Exception {
    Props props = new Props();

    String sqlScriptsDir = new File("../azkaban-db/src/main/sql/").getCanonicalPath();
    props.put("database.sql.scripts.dir", sqlScriptsDir);

    AzkabanDatabaseSetup setup = new AzkabanDatabaseSetup(dataSource, props);
    setup.loadTableInfo();
    setup.updateDatabase(true, false);

    CheckerTypeLoader checkerTypeLoader = new CheckerTypeLoader();
    ActionTypeLoader actionTypeLoader = new ActionTypeLoader();

    try {
      checkerTypeLoader.init(null);
      actionTypeLoader.init(null);
    } catch (Exception e) {
      throw new TriggerManagerException(e);
    }

    Condition.setCheckerLoader(checkerTypeLoader);
    Trigger.setActionTypeLoader(actionTypeLoader);

    checkerTypeLoader.registerCheckerType(BasicTimeChecker.type, BasicTimeChecker.class);
    actionTypeLoader.registerActionType(ExecuteFlowAction.type, ExecuteFlowAction.class);
  }

  @Before
  public void setUp() {

    dbOperator = new DatabaseOperatorImpl(new QueryRunner(dataSource));
    loader = new JdbcTriggerImpl(dbOperator);
  }

  @Test
  public void testRemoveTriggers() throws Exception {
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

  @Test
  public void testAddTrigger() throws Exception {
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

  @Test
  public void testUpdateTrigger() throws Exception {
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

  @After
  public void clearDB() {
    try {
      dbOperator.update("DELETE FROM triggers");

    } catch (SQLException e) {
      e.printStackTrace();
      return;
    }
  }
}