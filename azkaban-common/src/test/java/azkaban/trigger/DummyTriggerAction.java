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

import azkaban.trigger.TriggerAction;

public class DummyTriggerAction implements TriggerAction {

  public static final String type = "DummyAction";

  private String message;

  public DummyTriggerAction(String message) {
    this.message = message;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public TriggerAction fromJson(Object obj) {
    return null;
  }

  @Override
  public Object toJson() {
    return null;
  }

  @Override
  public void doAction() {
    System.out.println(getType() + " invoked.");
    System.out.println(message);
  }

  @Override
  public String getDescription() {
    return "this is real dummy action";
  }

  @Override
  public String getId() {
    return null;
  }

  @Override
  public void setContext(Map<String, Object> context) {
  }
}
