package azkaban.test.trigger;

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

}
