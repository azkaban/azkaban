package azkaban.trigger;

public interface TriggerAction {
	
	String getType();
	
	TriggerAction fromJson(Object obj);
	
	Object toJson();
	
	void doAction() throws Exception;
	
}
