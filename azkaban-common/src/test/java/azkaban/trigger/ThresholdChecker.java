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

import java.util.Map;

import azkaban.trigger.ConditionChecker;

public class ThresholdChecker implements ConditionChecker {

  private int threshold = -1;

  private static int curVal = -1;

  public static final String type = "ThresholdChecker";

  private String id;

  private boolean checkerMet = false;
  private boolean checkerReset = false;

  public ThresholdChecker(String id, int threshold) {
    this.id = id;
    this.threshold = threshold;
  }

  public synchronized static void setVal(int val) {
    curVal = val;
  }

  @Override
  public Boolean eval() {
    if (curVal > threshold) {
      checkerMet = true;
    }
    return checkerMet;
  }

  public boolean isCheckerMet() {
    return checkerMet;
  }

  @Override
  public void reset() {
    checkerMet = false;
    checkerReset = true;
  }

  public boolean isCheckerReset() {
    return checkerReset;
  }

  /*
   * TimeChecker format:
   * type_first-time-in-millis_next-time-in-millis_timezone_is
   * -recurring_skip-past-checks_period
   */
  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public ConditionChecker fromJson(Object obj) {
    return null;
  }

  @Override
  public Object getNum() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object toJson() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void stopChecker() {
    return;

  }

  @Override
  public void setContext(Map<String, Object> context) {
    // TODO Auto-generated method stub

  }

  @Override
  public long getNextCheckTime() {
    // TODO Auto-generated method stub
    return 0;
  }

}
