package azkaban.webapp.servlet;

import azkaban.trigger.TriggerAgent;

public interface TriggerPlugin {
	
//	public TriggerPlugin(String pluginName, Props props, AzkabanWebServer azkabanWebApp) {
//		this.pluginName = pluginName;
//		this.pluginPath = props.getString("trigger.path");
//		this.order = props.getInt("trigger.order", 0);
//		this.hidden = props.getBoolean("trigger.hidden", false);
//
//	}

	public AbstractAzkabanServlet getServlet();
	public TriggerAgent getAgent();
	public void load();
	
	public String getPluginName();

	public String getPluginPath();

	public int getOrder();
	
	public boolean isHidden();

	public void setHidden(boolean hidden);
	
	public String getInputPanelVM();
	
	
	
}
