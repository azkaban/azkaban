/* Copyright 2016 LinkedIn Corp.
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

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import azkaban.trigger.ConditionChecker;
import azkaban.trigger.TriggerManagerException;
import azkaban.utils.NyxUtils;


/***
 * Trigger checker leveraging upcoming Nyx service
 *
 * @author gaggarwa
 *
 */
public class NyxTriggerChecker implements ConditionChecker {
  private static Logger logger = Logger.getLogger(NyxTriggerChecker.class);

  public static final String type = "NyxTriggerChecker";

  private String specification;
  private String id;
  private long triggerId = -1L;

  public NyxTriggerChecker(String specification, String id) {
    this(specification, id, -1);
  }

  public NyxTriggerChecker(String specification, String id, long triggerId) {
    this.specification = specification;
    this.id = id;
    try {
      // register a trigger
      this.triggerId = (triggerId == -1 ? NyxUtils.registerNyxTrigger(specification) : triggerId);
    } catch (TriggerManagerException e) {
      throw new IllegalArgumentException(" Failed to register Trigger with the given spec." + e.getMessage(), e);
    }
  }

  public long getTriggerId() {
    return triggerId;
  }

  public Map<String, Object> getDetailedStatus() {
    try {
      if (triggerId == -1) {
        // if trigger is not registered then first register
        triggerId = NyxUtils.registerNyxTrigger(specification);
      }
      return NyxUtils.getNyxTriggerStatus(triggerId);
    } catch (TriggerManagerException ex) {
      logger.error("Error while getting the detailed status for the trigger " + id, ex);
      return null;
    }
  }

  @Override
  public Object eval() {
    try {
      if (triggerId == -1) {
        // if trigger is not registered then first register
        triggerId = NyxUtils.registerNyxTrigger(specification);
      }
      return NyxUtils.isNyxTriggerReady(triggerId);
    } catch (TriggerManagerException ex) {
      logger.error("Error while evaluating checker " + id, ex);
      return false;
    }
  }

  @Override
  public Object getNum() {
    return null;
  }

  @Override
  public void reset() {
    try {
      NyxUtils.unregisterNyxTrigger(triggerId);
      triggerId = NyxUtils.registerNyxTrigger(specification);
    } catch (TriggerManagerException ex) {
      logger.error("Error while resetting checker " + id, ex);
    }
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getType() {
    return type;
  }

  @SuppressWarnings("unchecked")
  @Override
  public ConditionChecker fromJson(Object obj) throws Exception {
    return createFromJson((HashMap<String, Object>) obj);
  }

  @Override
  public Object toJson() {
    Map<String, Object> jsonObj = new HashMap<String, Object>();
    jsonObj.put("type", type);
    jsonObj.put("specification", specification);
    jsonObj.put("triggerId", String.valueOf(triggerId));
    jsonObj.put("id", id);

    return jsonObj;
  }

  @Override
  public void stopChecker() {
    try {
      NyxUtils.unregisterNyxTrigger(triggerId);
    } catch (TriggerManagerException ex) {
      logger.error("Error while stopping checker " + id, ex);
    }
  }

  @Override
  public void setContext(Map<String, Object> context) {
    // Not applicable for Nyx trigger
  }

  @Override
  public long getNextCheckTime() {
    // Not applicable for Nyx trigger
    return Long.MAX_VALUE;
  }

  public static NyxTriggerChecker createFromJson(HashMap<String, Object> obj) throws Exception {
    Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    if (!jsonObj.get("type").equals(type)) {
      throw new Exception("Cannot create checker of " + type + " from " + jsonObj.get("type"));
    }
    Long triggerId = Long.valueOf((String) jsonObj.get("triggerId"));
    String id = (String) jsonObj.get("id");
    String specification = (String) jsonObj.get("specification");

    NyxTriggerChecker checker = new NyxTriggerChecker(specification, id, triggerId);
    return checker;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    result = prime * result + ((specification == null) ? 0 : specification.hashCode());
    result = prime * result + (int) (triggerId ^ (triggerId >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    NyxTriggerChecker other = (NyxTriggerChecker) obj;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    if (specification == null) {
      if (other.specification != null)
        return false;
    } else if (!specification.equals(other.specification))
      return false;
    if (triggerId != other.triggerId)
      return false;
    return true;
  }

}
