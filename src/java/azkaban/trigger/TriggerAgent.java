package azkaban.trigger;

import java.io.File;

import azkaban.utils.Props;

public interface TriggerAgent {
	public void loadTriggerFromProps(Props props) throws Exception;

	public String getTriggerSource();
	
	void load();

	public void updateLocal(Trigger t);

}
