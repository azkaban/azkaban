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

import static org.junit.Assert.assertTrue;

import azkaban.database.AzkabanConnectionPoolTest;
import azkaban.database.AzkabanDataSource;
import azkaban.database.AzkabanDatabaseSetup;
import azkaban.db.DatabaseOperator;
import azkaban.executor.ExecutionOptions;
import azkaban.trigger.builtin.BasicTimeChecker;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbutils.QueryRunner;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class JdbcTriggerImplTest {

  public static AzkabanDataSource dataSource = new AzkabanConnectionPoolTest.EmbeddedH2BasicDataSource();
  TriggerLoader loader;
  DatabaseOperator dbOperator;

  @BeforeClass
  public static void prepare() throws Exception {
    final Props props = new Props();

    final String sqlScriptsDir = new File("../azkaban-db/src/main/sql/").getCanonicalPath();
    props.put("database.sql.scripts.dir", sqlScriptsDir);

    final AzkabanDatabaseSetup setup = new AzkabanDatabaseSetup(dataSource, props);
    setup.loadTableInfo();
    setup.updateDatabase(true, false);

    final CheckerTypeLoader checkerTypeLoader = new CheckerTypeLoader();
    final ActionTypeLoader actionTypeLoader = new ActionTypeLoader();

    try {
      checkerTypeLoader.init(null);
      actionTypeLoader.init(null);
    } catch (final Exception e) {
      throw new TriggerManagerException(e);
    }

    Condition.setCheckerLoader(checkerTypeLoader);
    Trigger.setActionTypeLoader(actionTypeLoader);

    checkerTypeLoader.registerCheckerType(BasicTimeChecker.type, BasicTimeChecker.class);
    actionTypeLoader.registerActionType(ExecuteFlowAction.type, ExecuteFlowAction.class);
  }

  @Before
  public void setUp() {

    this.dbOperator = new DatabaseOperator(new QueryRunner(dataSource));
    this.loader = new JdbcTriggerImpl(this.dbOperator);
  }

  @Test
  public void testRemoveTriggers() throws Exception {
    final Trigger t1 = createTrigger("testProj1", "testFlow1", "source1");
    final Trigger t2 = createTrigger("testProj2", "testFlow2", "source2");
    this.loader.addTrigger(t1);
    this.loader.addTrigger(t2);
    List<Trigger> ts = this.loader.loadTriggers();
    assertTrue(ts.size() == 2);
    this.loader.removeTrigger(t2);
    ts = this.loader.loadTriggers();
    assertTrue(ts.size() == 1);
    assertTrue(ts.get(0).getTriggerId() == t1.getTriggerId());
  }

  @Test
  public void testAddTrigger() throws Exception {
    final Trigger t1 = createTrigger("testProj1", "testFlow1", "source1");
    final Trigger t2 = createTrigger("testProj2", "testFlow2", "source2");
    this.loader.addTrigger(t1);

    List<Trigger> ts = this.loader.loadTriggers();
    assertTrue(ts.size() == 1);

    final Trigger t3 = ts.get(0);
    assertTrue(t3.getSource().equals("source1"));

    this.loader.addTrigger(t2);
    ts = this.loader.loadTriggers();
    assertTrue(ts.size() == 2);

    for (final Trigger t : ts) {
      if (t.getTriggerId() == t2.getTriggerId()) {
        t.getSource().equals(t2.getSource());
      }
    }
  }

  @Test
  public void testUpdateTrigger() throws Exception {
    final Trigger t1 = createTrigger("testProj1", "testFlow1", "source1");
    t1.setResetOnExpire(true);
    this.loader.addTrigger(t1);
    List<Trigger> ts = this.loader.loadTriggers();
    assertTrue(ts.get(0).isResetOnExpire() == true);
    t1.setResetOnExpire(false);
    this.loader.updateTrigger(t1);
    ts = this.loader.loadTriggers();
    assertTrue(ts.get(0).isResetOnExpire() == false);
  }

  private Trigger createTrigger(final String projName, final String flowName, final String source) {
    final DateTime now = DateTime.now();
    final ConditionChecker checker1 =
        new BasicTimeChecker("timeChecker1", now.getMillis(), now.getZone(),
            true, true, Utils.parsePeriodString("1h"), null);
    final Map<String, ConditionChecker> checkers1 =
        new HashMap<>();
    checkers1.put(checker1.getId(), checker1);
    final String expr1 = checker1.getId() + ".eval()";
    final Condition triggerCond = new Condition(checkers1, expr1);
    final Condition expireCond = new Condition(checkers1, expr1);
    final List<TriggerAction> actions = new ArrayList<>();
    final TriggerAction action =
        new ExecuteFlowAction("executeAction", 1, projName, flowName,
            "azkaban", new ExecutionOptions(), null);
    actions.add(action);

    final Trigger t = new Trigger.TriggerBuilder("azkaban",
        source,
        triggerCond,
        expireCond,
        actions)
        .build();

    return t;
  }

  @After
  public void clearDB() {
    try {
      this.dbOperator.update("DELETE FROM triggers");

    } catch (final SQLException e) {
      e.printStackTrace();
      return;
    }
  }
}