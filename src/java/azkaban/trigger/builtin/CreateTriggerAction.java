package azkaban.trigger.builtin;

import java.util.HashMap;
import java.util.Map;

import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.triggerapp.TriggerRunnerManager;

public class CreateTriggerAction implements TriggerAction {
	
	public static final String type = "CreateTriggerAction";
	private static TriggerRunnerManager triggerRunnerManager;
	private Trigger trigger;

	public CreateTriggerAction(Trigger trigger) {
		this.trigger = trigger;
	}
	
	@Override
	public String getType() {
		return type;
	}
	
	public static void setTriggerRunnerManager(TriggerRunnerManager trm) {
		triggerRunnerManager = trm;
	}

	@SuppressWarnings("unchecked")
	public static CreateTriggerAction createFromJson(Object obj) throws Exception {
		Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
		if(!jsonObj.get("type").equals(type)) {
			throw new Exception("Cannot create action of " + type + " from " + jsonObj.get("type"));
		}
		Trigger trigger = Trigger.fromJson(jsonObj.get("trigger"));
		return new CreateTriggerAction(trigger);
	}
	
	@Override
	public CreateTriggerAction fromJson(Object obj) throws Exception {
		// TODO Auto-generated method stub
		return createFromJson(obj);
	}

	@Override
	public Object toJson() {
		Map<String, Object> jsonObj = new HashMap<String, Object>();
		jsonObj.put("type", type);
		jsonObj.put("trigger", trigger.toJson());

		return jsonObj;
	}

	@Override
	public void doAction() throws Exception {
		triggerRunnerManager.insertTrigger(trigger);
	}

	@Override
	public String getDescription() {
		return "create another: " + trigger.getDescription();
	}

}
