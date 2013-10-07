package azkaban.trigger;

import java.util.Map;


public interface ConditionChecker {
	
	Object eval();
	
	Object getNum();
	
	void reset();

	String getId();
	
	String getType();
	
	ConditionChecker fromJson(Object obj) throws Exception;
	
	Object toJson();

	void stopChecker();
	
	void setContext(Map<String, Object> context);
	
	long getNextCheckTime();
	
//	void setCondition(Condition c);
//	
//	String getDescription();
}
