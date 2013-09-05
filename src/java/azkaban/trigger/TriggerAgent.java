package azkaban.trigger;

import azkaban.utils.Props;

public interface TriggerAgent {
	public void loadTriggerFromProps(Props props) throws Exception;

	public String getTriggerSource();
	
	public void start() throws Exception;
	
	public void shutdown();

}
