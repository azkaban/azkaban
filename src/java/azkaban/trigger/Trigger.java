package azkaban.trigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class Trigger {
	
	private static Logger logger = Logger.getLogger(Trigger.class);
	
	private int triggerId = -1;
	private long lastModifyTime = -1;
	private long submitTime = -1;
	private String submitUser;
	
	private Condition triggerCondition;
	private Condition expireCondition;
	private List<TriggerAction> actions;
	
	private static ActionTypeLoader actionTypeLoader;
	
	private boolean resetOnTrigger = false;
	private boolean resetOnExpire = false;
	
	@SuppressWarnings("unused")
	private Trigger() throws TriggerManagerException {	
		throw new TriggerManagerException("Triggers should always be specified");
	}
	
	public Trigger(
			long lastModifyTime, 
			long submitTime, 
			String submitUser, 
			Condition triggerCondition,
			Condition expireCondition,
			List<TriggerAction> actions) {
		this.lastModifyTime = lastModifyTime;
		this.submitTime = submitTime;
		this.submitUser = submitUser;
		this.triggerCondition = triggerCondition;
		this.expireCondition = expireCondition;
		this.actions = actions;
	}
	
	public Trigger(
			int triggerId,
			long lastModifyTime, 
			long submitTime, 
			String submitUser, 
			Condition triggerCondition,
			Condition expireCondition,
			List<TriggerAction> actions) {
		this.triggerId = triggerId;
		this.lastModifyTime = lastModifyTime;
		this.submitTime = submitTime;
		this.submitUser = submitUser;
		this.triggerCondition = triggerCondition;
		this.expireCondition = expireCondition;
		this.actions = actions;
	}
	
	public static synchronized void setActionTypeLoader(ActionTypeLoader loader) {
		Trigger.actionTypeLoader = loader;
	}
	
	public boolean isResetOnTrigger() {
		return resetOnTrigger;
	}
	
	public void setResetOnTrigger(boolean resetOnTrigger) {
		this.resetOnTrigger = resetOnTrigger;
	}
	
	public boolean isResetOnExpire() {
		return resetOnExpire;
	}
	
	public void setResetOnExpire(boolean resetOnExpire) {
		this.resetOnExpire = resetOnExpire;
	}

	public long getLastModifyTime() {
		return lastModifyTime;
	}

	public void setTriggerId(int id) {
		this.triggerId = id;
	}
	
	public int getTriggerId() {
		return triggerId;
	}

	public boolean triggerConditionMet(){
		return triggerCondition.isMet();
	}
	
	public boolean expireConditionMet(){
		return expireCondition.isMet();
	}
	
	public void resetTriggerConditions() {
		triggerCondition.resetCheckers();
	}
	
	public List<TriggerAction> getTriggerActions () {
		return actions;
	}
	
	public Map<String, Object> toJson() {
		Map<String, Object> jsonObj = new HashMap<String, Object>();
		jsonObj.put("triggerCondition", triggerCondition.toJson());
		jsonObj.put("expireCondition", expireCondition.toJson());
		List<Object> actionsJson = new ArrayList<Object>();
		for(TriggerAction action : actions) {
			Map<String, Object> oneActionJson = new HashMap<String, Object>();
			oneActionJson.put("type", action.getType());
			oneActionJson.put("actionJson", action.toJson());
			actionsJson.add(oneActionJson);
		}
		jsonObj.put("actions", actionsJson);
		jsonObj.put("resetOnTrigger", String.valueOf(resetOnTrigger));
		jsonObj.put("resetOnExpire", String.valueOf(resetOnExpire));
		jsonObj.put("submitUser", submitUser);
		jsonObj.put("submitTime", String.valueOf(submitTime));
		jsonObj.put("lastModifyTime", String.valueOf(lastModifyTime));
		jsonObj.put("triggerId", String.valueOf(triggerId));
		
		return jsonObj;
	}
	
	
	@SuppressWarnings("unchecked")
	public static Trigger fromJson(Object obj) {
		
		Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
		
		Trigger trigger = null;
		try{
			Condition triggerCond = Condition.fromJson(jsonObj.get("triggerCondition"));
			Condition expireCond = Condition.fromJson(jsonObj.get("expireCondition"));
			List<TriggerAction> actions = new ArrayList<TriggerAction>();
			List<Object> actionsJson = (List<Object>) jsonObj.get("actions");
			for(Object actObj : actionsJson) {
				Map<String, Object> oneActionJson = (HashMap<String, Object>) actObj;
				String type = (String) oneActionJson.get("type");
				TriggerAction act = actionTypeLoader.createActionFromJson(type, oneActionJson.get("actionJson"));
				actions.add(act);
			}
			boolean resetOnTrigger = Boolean.valueOf((String) jsonObj.get("resetOnTrigger"));
			boolean resetOnExpire = Boolean.valueOf((String) jsonObj.get("resetOnExpire"));
			String submitUser = (String) jsonObj.get("submitUser");
			long submitTime = Long.valueOf((String) jsonObj.get("submitTime"));
			long lastModifyTime = Long.valueOf((String) jsonObj.get("lastModifyTime"));
			int triggerId = Integer.valueOf((String) jsonObj.get("triggerId"));
			trigger = new Trigger(triggerId, lastModifyTime, submitTime, submitUser, triggerCond, expireCond, actions);
			trigger.setResetOnExpire(resetOnExpire);
			trigger.setResetOnTrigger(resetOnTrigger);
		}catch(Exception e) {
			e.printStackTrace();
			logger.error("Failed to decode the trigger.", e);
			return null;
		}
		
		return trigger;
	}
	
}
