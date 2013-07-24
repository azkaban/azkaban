package azkaban.trigger;

public interface TriggerAction {
	
	String getType();
	
	TriggerAction fromJson(Object obj) throws Exception;
	
	Object toJson();
	
	void doAction() throws Exception;

	String getDescription();
	
}
