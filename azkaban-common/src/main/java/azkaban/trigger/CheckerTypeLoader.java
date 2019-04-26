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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CheckerTypeLoader {

  public static final String DEFAULT_CONDITION_CHECKER_PLUGIN_DIR =
      "plugins/conditioncheckers";
  private static final Logger LOG = LoggerFactory.getLogger(CheckerTypeLoader.class);
  protected static Map<String, Class<? extends ConditionChecker>> checkerToClass =
      new HashMap<>();

  public static void registerBuiltinCheckers(
      final Map<String, Class<? extends ConditionChecker>> builtinCheckers) {
    checkerToClass.putAll(checkerToClass);
    for (final String type : builtinCheckers.keySet()) {
      LOG.info("Loaded " + type + " checker.");
    }
  }

  public void init(final Props props) throws TriggerException {
  }

  public synchronized void registerCheckerType(final String type,
      final Class<? extends ConditionChecker> checkerClass) {
    LOG.info("Registering checker " + type);
    if (!checkerToClass.containsKey(type)) {
      checkerToClass.put(type, checkerClass);
    }
  }

  public ConditionChecker createCheckerFromJson(final String type, final Object obj)
      throws Exception {
    ConditionChecker checker = null;
    final Class<? extends ConditionChecker> checkerClass = checkerToClass.get(type);
    if (checkerClass == null) {
      throw new Exception("Checker type " + type + " not supported!");
    }
    checker =
        (ConditionChecker) Utils.invokeStaticMethod(
            checkerClass.getClassLoader(), checkerClass.getName(),
            "createFromJson", obj);

    return checker;
  }

  public ConditionChecker createChecker(final String type, final Object... args) {
    ConditionChecker checker = null;
    final Class<? extends ConditionChecker> checkerClass = checkerToClass.get(type);
    checker = (ConditionChecker) Utils.callConstructor(checkerClass, args);

    return checker;
  }

  public Map<String, Class<? extends ConditionChecker>> getSupportedCheckers() {
    return checkerToClass;
  }

}
