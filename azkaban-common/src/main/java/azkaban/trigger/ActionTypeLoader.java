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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import azkaban.utils.Props;
import azkaban.utils.Utils;

public class ActionTypeLoader {

  private static Logger logger = Logger.getLogger(ActionTypeLoader.class);

  public static final String DEFAULT_TRIGGER_ACTION_PLUGIN_DIR =
      "plugins/triggeractions";

  protected static Map<String, Class<? extends TriggerAction>> actionToClass =
      new HashMap<String, Class<? extends TriggerAction>>();

  public void init(Props props) throws TriggerException {
  }

  public synchronized void registerActionType(String type,
      Class<? extends TriggerAction> actionClass) {
    logger.info("Registering action " + type);
    if (!actionToClass.containsKey(type)) {
      actionToClass.put(type, actionClass);
    }
  }

  public static void registerBuiltinActions(
      Map<String, Class<? extends TriggerAction>> builtinActions) {
    actionToClass.putAll(builtinActions);
    for (String type : builtinActions.keySet()) {
      logger.info("Loaded " + type + " action.");
    }
  }

  public TriggerAction createActionFromJson(String type, Object obj)
      throws Exception {
    TriggerAction action = null;
    Class<? extends TriggerAction> actionClass = actionToClass.get(type);
    if (actionClass == null) {
      throw new Exception("Action Type " + type + " not supported!");
    }
    action =
        (TriggerAction) Utils.invokeStaticMethod(actionClass.getClassLoader(),
            actionClass.getName(), "createFromJson", obj);

    return action;
  }

  public TriggerAction createAction(String type, Object... args) {
    TriggerAction action = null;
    Class<? extends TriggerAction> actionClass = actionToClass.get(type);
    action = (TriggerAction) Utils.callConstructor(actionClass, args);

    return action;
  }

  public Set<String> getSupportedActions() {
    return actionToClass.keySet();
  }
}
