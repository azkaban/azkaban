package azkaban.trigger;

import java.io.File;

import azkaban.utils.Props;

public interface TriggerServicer {
	public void createTriggerFromProps(Props props) throws Exception;

	public String getTriggerSource();
	
	void load();

}
