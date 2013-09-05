package azkaban.trigger.builtin;

import java.util.HashMap;
import java.util.Map;

import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.TriggerManager;

public class CreateTriggerAction implements TriggerAction {
	
	public static final String type = "CreateTriggerAction";
	private static TriggerManager triggerManager;
	private Trigger trigger;
	private Map<String, Object> context;
	private String actionId;
	
	public CreateTriggerAction(String actionId, Trigger trigger) {
		this.actionId = actionId;
		this.trigger = trigger;
	}
	
	@Override
	public String getType() {
		return type;
	}
	
	public static void setTriggerManager(TriggerManager trm) {
		triggerManager = trm;
	}

	@SuppressWarnings("unchecked")
	public static CreateTriggerAction createFromJson(Object obj) throws Exception {
		Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
		if(!jsonObj.get("type").equals(type)) {
			throw new Exception("Cannot create action of " + type + " from " + jsonObj.get("type"));
		}
		String actionId = (String) jsonObj.get("actionId");
		Trigger trigger = Trigger.fromJson(jsonObj.get("trigger"));
		return new CreateTriggerAction(actionId, trigger);
	}
	
	@Override
	public CreateTriggerAction fromJson(Object obj) throws Exception {
		// TODO Auto-generated method stub
		return createFromJson(obj);
	}

	@Override
	public Object toJson() {
		Map<String, Object> jsonObj = new HashMap<String, Object>();
		jsonObj.put("actionId", actionId);
		jsonObj.put("type", type);
		jsonObj.put("trigger", trigger.toJson());

		return jsonObj;
	}

	@Override
	public void doAction() throws Exception {
		triggerManager.insertTrigger(trigger);
	}

	@Override
	public String getDescription() {
		return "create another: " + trigger.getDescription();
	}

	@Override
	public String getId() {
		return actionId;
	}

	@Override
	public void setContext(Map<String, Object> context) {
		this.context = context;
	}

}
