package azkaban.webapp.servlet;

public class ViewerPlugin {
	private final String pluginName;
	private final String pluginPath;
	private final int order;
	private boolean hidden;
	
	public ViewerPlugin(String pluginName, String pluginPath, int order, boolean hidden) {
		this.pluginName = pluginName;
		this.pluginPath = pluginPath;
		this.order = order;
		this.setHidden(hidden);
	}

	public String getPluginName() {
		return pluginName;
	}

	public String getPluginPath() {
		return pluginPath;
	}

	public int getOrder() {
		return order;
	}

	public boolean isHidden() {
		return hidden;
	}

	public void setHidden(boolean hidden) {
		this.hidden = hidden;
	}
}
