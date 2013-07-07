package azkaban.trigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

public class Trigger {
	
	private static Logger logger = Logger.getLogger(Trigger.class);
	
	private int triggerId = -1;
	private DateTime lastModifyTime;
	private DateTime submitTime;
	private String submitUser;
	private String source;
	private TriggerStatus status = TriggerStatus.READY;
	
	private Condition triggerCondition;
	private Condition expireCondition;
	private List<TriggerAction> actions;
	
	private static ActionTypeLoader actionTypeLoader;
	
	private boolean resetOnTrigger = true;
	private boolean resetOnExpire = true;
	
	@SuppressWarnings("unused")
	private Trigger() throws TriggerManagerException {	
		throw new TriggerManagerException("Triggers should always be specified");
	}
	
	public DateTime getSubmitTime() {
		return submitTime;
	}

	public String getSubmitUser() {
		return submitUser;
	}

	public TriggerStatus getStatus() {
		return status;
	}

	public void setStatus(TriggerStatus status) {
		this.status = status;
	}

	public Condition getTriggerCondition() {
		return triggerCondition;
	}

	public Condition getExpireCondition() {
		return expireCondition;
	}

	public List<TriggerAction> getActions() {
		return actions;
	}

	public Trigger(
			DateTime lastModifyTime, 
			DateTime submitTime, 
			String submitUser, 
			String source,
			Condition triggerCondition,
			Condition expireCondition,
			List<TriggerAction> actions) {
		this.lastModifyTime = lastModifyTime;
		this.submitTime = submitTime;
		this.submitUser = submitUser;
		this.source = source;
		this.triggerCondition = triggerCondition;
		this.expireCondition = expireCondition;
		this.actions = actions;
	}
	
	public Trigger(
			int triggerId,
			DateTime lastModifyTime, 
			DateTime submitTime, 
			String submitUser, 
			String source,
			Condition triggerCondition,
			Condition expireCondition,
			List<TriggerAction> actions) {
		this.triggerId = triggerId;
		this.lastModifyTime = lastModifyTime;
		this.submitTime = submitTime;
		this.submitUser = submitUser;
		this.source = source;
		this.triggerCondition = triggerCondition;
		this.expireCondition = expireCondition;
		this.actions = actions;
	}
	
	public static synchronized void setActionTypeLoader(ActionTypeLoader loader) {
		Trigger.actionTypeLoader = loader;
	}
	
	public static ActionTypeLoader getActionTypeLoader() {
		return actionTypeLoader;
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

	public DateTime getLastModifyTime() {
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
	
	public void resetExpireCondition() {
		expireCondition.resetCheckers();
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
		jsonObj.put("source", source);
		jsonObj.put("submitTime", String.valueOf(submitTime.getMillis()));
		jsonObj.put("lastModifyTime", String.valueOf(lastModifyTime.getMillis()));
		jsonObj.put("triggerId", String.valueOf(triggerId));
		jsonObj.put("status", status.toString());
		
		return jsonObj;
	}
	
	
	public String getSource() {
		return source;
	}

	@SuppressWarnings("unchecked")
	public static Trigger fromJson(Object obj) throws Exception {
		
		if(actionTypeLoader == null) {
			throw new Exception("Trigger Action Type loader not initialized.");
		}
 		
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
			String source = (String) jsonObj.get("source");
			long submitTimeMillis = Long.valueOf((String) jsonObj.get("submitTime"));
			long lastModifyTimeMillis = Long.valueOf((String) jsonObj.get("lastModifyTime"));
			DateTime submitTime = new DateTime(submitTimeMillis);
			DateTime lastModifyTime = new DateTime(lastModifyTimeMillis);
			int triggerId = Integer.valueOf((String) jsonObj.get("triggerId"));
			TriggerStatus status = TriggerStatus.valueOf((String)jsonObj.get("status"));
			trigger = new Trigger(triggerId, lastModifyTime, submitTime, submitUser, source, triggerCond, expireCond, actions);
			trigger.setResetOnExpire(resetOnExpire);
			trigger.setResetOnTrigger(resetOnTrigger);
			trigger.setStatus(status);
		}catch(Exception e) {
			e.printStackTrace();
			logger.error("Failed to decode the trigger.", e);
			return null;
		}
		
		return trigger;
	}

	public String getDescription() {
		StringBuffer actionsString = new StringBuffer();
		for(TriggerAction act : actions) {
			actionsString.append(", ");
			actionsString.append(act.getDescription());
		}
		return "Trigger from " + getSource() +
				" with trigger condition of " + triggerCondition.getExpression() +
				" and expire condition of " + expireCondition.getExpression() + 
				actionsString;
	}

	public void stopCheckers() {
		for(ConditionChecker checker : triggerCondition.getCheckers().values()) {
			checker.stopChecker();
		}
		for(ConditionChecker checker : expireCondition.getCheckers().values()) {
			checker.stopChecker();
		}
		
	}

	
}
