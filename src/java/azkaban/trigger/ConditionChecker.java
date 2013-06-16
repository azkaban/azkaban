package azkaban.trigger;


public interface ConditionChecker {
	
	Object eval();
	
	Object getNum();
	
	void reset();
	
	void setId(String id);
	
	String getId();
	
	String getType();
	
	ConditionChecker fromJson(Object obj) throws Exception;
	
	Object toJson();

}
