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

package azkaban.trigger;

import azkaban.trigger.builtin.BasicTimeChecker;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;


public class Condition {

  private static final Logger logger = Logger.getLogger(Condition.class);

  private static final JexlEngine jexl = new JexlEngine();
  private static CheckerTypeLoader checkerLoader = null;
  private final MapContext context = new MapContext();
  private Expression expression;
  private Map<String, ConditionChecker> checkers = new HashMap<>();
  private Long nextCheckTime = -1L;
  private List<Long> missedCheckTimes = new ArrayList<>();

  /**
   * Used when creating a new trigger instance (both triggerCond and expireCond) from schedule,
   * it does not need to count missed schedules.
   * */
  public Condition(final Map<String, ConditionChecker> checkers, final String expr) {
    setCheckers(checkers);
    this.expression = jexl.createExpression(expr);
    updateNextCheckTime();
  }

  /**
   * Used when reload Trigger from DB.
   * It's possible to have missed schedule from last check point.
   * */
  public Condition(final Map<String, ConditionChecker> checkers, final String expr, final long nextCheckTime,
      final List<Long> missedCheckTimes) {
    this.nextCheckTime = nextCheckTime;
    this.missedCheckTimes = missedCheckTimes;
    setCheckers(checkers);
    this.expression = jexl.createExpression(expr);
  }

  public synchronized static void setCheckerLoader(final CheckerTypeLoader loader) {
    Condition.checkerLoader = loader;
  }

  public static Condition fromJson(final Object obj) throws Exception {
    if (checkerLoader == null) {
      throw new Exception("Condition Checker loader not initialized!");
    }

    final Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    Condition cond = null;
    List<Long> missedCheckTimes = new ArrayList<>();
    try {
      final Map<String, ConditionChecker> checkers = new HashMap<>();
      final List<Object> checkersJson = (List<Object>) jsonObj.get("checkers");
      for (final Object oneCheckerJson : checkersJson) {
        final Map<String, Object> oneChecker = (HashMap<String, Object>) oneCheckerJson;
        final String type = (String) oneChecker.get("type");
        final ConditionChecker ck = checkerLoader.createCheckerFromJson(type, oneChecker.get("checkerJson"));
        checkers.put(ck.getId(), ck);
        // there is only one BasicTimeChecker associated with each condition
        if (ck instanceof BasicTimeChecker && ck.getId().equals("BasicTimeChecker_1")) {
          missedCheckTimes = (((BasicTimeChecker) ck).getMissedCheckTimesBeforeNow());
        }
      }
      final String expr = (String) jsonObj.get("expression");
      final Long nextCheckTime = Long.valueOf((String) jsonObj.get("nextCheckTime"));

      cond = new Condition(checkers, expr, nextCheckTime, missedCheckTimes);
    } catch (final Exception e) {
      e.printStackTrace();
      logger.error("Failed to recreate condition from json.", e);
      throw new Exception("Failed to recreate condition from json.", e);
    }

    return cond;
  }

  public long getNextCheckTime() {
    return this.nextCheckTime;
  }

  public List<Long> getMissedCheckTimes() {
    return this.missedCheckTimes;
  }

  public Map<String, ConditionChecker> getCheckers() {
    return this.checkers;
  }

  private void setCheckers(final Map<String, ConditionChecker> checkers) {
    this.checkers = checkers;
    for (final ConditionChecker checker : checkers.values()) {
      this.context.set(checker.getId(), checker);
    }
    updateNextCheckTime();
  }

  private void updateNextCheckTime() {
    long time = Long.MAX_VALUE;
    for (final ConditionChecker checker : this.checkers.values()) {
      time = Math.min(time, checker.getNextCheckTime());
    }
    this.nextCheckTime = time;
  }

  public void resetCheckers() {
    for (final ConditionChecker checker : this.checkers.values()) {
      checker.reset();
      if (checker instanceof BasicTimeChecker) {
        this.missedCheckTimes = ((BasicTimeChecker) checker).getMissedCheckTimesBeforeNow();
      }
    }
    updateNextCheckTime();
    logger.info("Done resetting checkers. The next check time will be " + new DateTime(this.nextCheckTime));
  }

  public String getExpression() {
    return this.expression.getExpression();
  }

  public void setExpression(final String expr) {
    this.expression = jexl.createExpression(expr);
  }

  public boolean isMet() {
    if (logger.isDebugEnabled()) {
      logger.debug("Testing condition " + this.expression);
    }
    return this.expression.evaluate(this.context).equals(Boolean.TRUE);
  }

  public Object toJson() {
    final Map<String, Object> jsonObj = new HashMap<>();
    jsonObj.put("expression", this.expression.getExpression());

    final List<Object> checkersJson = new ArrayList<>();
    for (final ConditionChecker checker : this.checkers.values()) {
      final Map<String, Object> oneChecker = new HashMap<>();
      oneChecker.put("type", checker.getType());
      oneChecker.put("checkerJson", checker.toJson());
      checkersJson.add(oneChecker);
    }
    jsonObj.put("checkers", checkersJson);
    jsonObj.put("nextCheckTime", String.valueOf(this.nextCheckTime));

    return jsonObj;
  }
}
