package azkaban.trigger;

import azkaban.utils.Props;

public interface TriggerAgent {
	void loadTriggerFromProps(Props props) throws Exception;

	String getTriggerSource();
	
	void start() throws Exception;

}
