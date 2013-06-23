package azkaban.webapp.servlet;

import azkaban.trigger.TriggerServicer;

public interface TriggerPlugin {
	
//	public TriggerPlugin(String pluginName, Props props, AzkabanWebServer azkabanWebApp) {
//		this.pluginName = pluginName;
//		this.pluginPath = props.getString("trigger.path");
//		this.order = props.getInt("trigger.order", 0);
//		this.hidden = props.getBoolean("trigger.hidden", false);
//
//	}

	public AbstractAzkabanServlet getServlet();
	public TriggerServicer getServicer();
	public void load();
}
