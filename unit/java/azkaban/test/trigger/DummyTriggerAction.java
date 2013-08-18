package azkaban.test.trigger;

import java.util.Map;

import azkaban.trigger.TriggerAction;

public class DummyTriggerAction implements TriggerAction{

	public static final String type = "DummyAction";
	
	private String message;
	
	public DummyTriggerAction(String message) {
		this.message = message;
	}
	
	@Override
	public String getType() {
		return type;
	}

	@Override
	public TriggerAction fromJson(Object obj) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object toJson() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void doAction() {
		System.out.println(getType() + " invoked.");
		System.out.println(message);
	}

	@Override
	public String getDescription() {
		return "this is real dummy action";
	}

	@Override
	public String getId() {
		return null;
	}

	@Override
	public void setContext(Map<String, Object> context) {
		// TODO Auto-generated method stub
		
	}

}
