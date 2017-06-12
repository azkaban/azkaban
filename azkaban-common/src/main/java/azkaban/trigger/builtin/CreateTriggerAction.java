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

package azkaban.trigger.builtin;

import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.TriggerManager;
import java.util.HashMap;
import java.util.Map;

public class CreateTriggerAction implements TriggerAction {

  public static final String type = "CreateTriggerAction";
  private static TriggerManager triggerManager;
  private final Trigger trigger;
  private final String actionId;
  private Map<String, Object> context;

  public CreateTriggerAction(final String actionId, final Trigger trigger) {
    this.actionId = actionId;
    this.trigger = trigger;
  }

  public static void setTriggerManager(final TriggerManager trm) {
    triggerManager = trm;
  }

  public static CreateTriggerAction createFromJson(final Object obj) throws Exception {
    final Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    if (!jsonObj.get("type").equals(type)) {
      throw new Exception("Cannot create action of " + type + " from "
          + jsonObj.get("type"));
    }
    final String actionId = (String) jsonObj.get("actionId");
    final Trigger trigger = Trigger.fromJson(jsonObj.get("trigger"));
    return new CreateTriggerAction(actionId, trigger);
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public CreateTriggerAction fromJson(final Object obj) throws Exception {
    return createFromJson(obj);
  }

  @Override
  public Object toJson() {
    final Map<String, Object> jsonObj = new HashMap<>();
    jsonObj.put("actionId", this.actionId);
    jsonObj.put("type", type);
    jsonObj.put("trigger", this.trigger.toJson());

    return jsonObj;
  }

  @Override
  public void doAction() throws Exception {
    triggerManager.insertTrigger(this.trigger);
  }

  @Override
  public String getDescription() {
    return "create another: " + this.trigger.getDescription();
  }

  @Override
  public String getId() {
    return this.actionId;
  }

  @Override
  public void setContext(final Map<String, Object> context) {
    this.context = context;
  }

}
