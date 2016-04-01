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

  public NyxTriggerChecker(String specification, String id)
      throws TriggerManagerException {
    this(specification, id, -1);
    // TODO: register a trigger
  }

  public NyxTriggerChecker(String specification, String id, long triggerId)
      throws TriggerManagerException {
    this.specification = specification;
    this.id = id;
    this.triggerId = (triggerId == -1
        ? NyxUtils.registerNyxTrigger(specification) : triggerId);
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

  public static NyxTriggerChecker createFromJson(HashMap<String, Object> obj)
      throws Exception {
    Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    if (!jsonObj.get("type").equals(type)) {
      throw new Exception(
          "Cannot create checker of " + type + " from " + jsonObj.get("type"));
    }
    Long triggerId = Long.valueOf((String) jsonObj.get("triggerId"));
    String id = (String) jsonObj.get("id");
    String specification = (String) jsonObj.get("specification");

    NyxTriggerChecker checker =
        new NyxTriggerChecker(specification, id, triggerId);
    return checker;
  }

}
