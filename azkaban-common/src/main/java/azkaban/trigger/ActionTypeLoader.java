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

import azkaban.utils.Props;
import azkaban.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ActionTypeLoader {

  public static final String DEFAULT_TRIGGER_ACTION_PLUGIN_DIR =
      "plugins/triggeractions";
  private static final Logger LOG = LoggerFactory.getLogger(ActionTypeLoader.class);
  protected static Map<String, Class<? extends TriggerAction>> actionToClass =
      new HashMap<>();

  public static void registerBuiltinActions(
      final Map<String, Class<? extends TriggerAction>> builtinActions) {
    actionToClass.putAll(builtinActions);
    for (final String type : builtinActions.keySet()) {
      LOG.info("Loaded " + type + " action.");
    }
  }

  public void init(final Props props) throws TriggerException {
  }

  public synchronized void registerActionType(final String type,
      final Class<? extends TriggerAction> actionClass) {
    LOG.info("Registering action " + type);
    if (!actionToClass.containsKey(type)) {
      actionToClass.put(type, actionClass);
    }
  }

  public TriggerAction createActionFromJson(final String type, final Object obj)
      throws Exception {
    TriggerAction action = null;
    final Class<? extends TriggerAction> actionClass = actionToClass.get(type);
    if (actionClass == null) {
      throw new Exception("Action Type " + type + " not supported!");
    }
    action =
        (TriggerAction) Utils.invokeStaticMethod(actionClass.getClassLoader(),
            actionClass.getName(), "createFromJson", obj);

    return action;
  }

  public TriggerAction createAction(final String type, final Object... args) {
    TriggerAction action = null;
    final Class<? extends TriggerAction> actionClass = actionToClass.get(type);
    action = (TriggerAction) Utils.callConstructor(actionClass, args);

    return action;
  }

  public Set<String> getSupportedActions() {
    return actionToClass.keySet();
  }
}
