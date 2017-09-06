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

public class ThresholdChecker implements ConditionChecker {

  public static final String type = "ThresholdChecker";
  private static int curVal = -1;
  private final String id;
  private int threshold = -1;
  private boolean checkerMet = false;
  private boolean checkerReset = false;

  public ThresholdChecker(final String id, final int threshold) {
    this.id = id;
    this.threshold = threshold;
  }

  public synchronized static void setVal(final int val) {
    curVal = val;
  }

  @Override
  public Boolean eval() {
    if (curVal > this.threshold) {
      this.checkerMet = true;
    }
    return this.checkerMet;
  }

  public boolean isCheckerMet() {
    return this.checkerMet;
  }

  @Override
  public void reset() {
    this.checkerMet = false;
    this.checkerReset = true;
  }

  public boolean isCheckerReset() {
    return this.checkerReset;
  }

  /*
   * TimeChecker format:
   * type_first-time-in-millis_next-time-in-millis_timezone_is
   * -recurring_skip-past-checks_period
   */
  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public ConditionChecker fromJson(final Object obj) {
    return null;
  }

  @Override
  public Object getNum() {
    return null;
  }

  @Override
  public Object toJson() {
    return null;
  }

  @Override
  public void stopChecker() {
    return;
  }

  @Override
  public void setContext(final Map<String, Object> context) {
  }

  @Override
  public long getNextCheckTime() {
    return 0;
  }

}
