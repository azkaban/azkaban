package azkaban.trigger;

import java.io.File;

import azkaban.utils.Props;

public interface TriggerAgent {
	public void loadTriggerFromProps(Props props) throws Exception;

	public String getTriggerSource();
	
	void load();

//	// update local copy
//	public void updateLocal();

}
