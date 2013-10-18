package azkaban.trigger;

import java.util.Map;

public interface TriggerAction {
	
	String getId();
	
	String getType();
	
	TriggerAction fromJson(Object obj) throws Exception;
	
	Object toJson();
	
	void doAction() throws Exception;
	
	void setContext(Map<String, Object> context);

	String getDescription();
	
}
