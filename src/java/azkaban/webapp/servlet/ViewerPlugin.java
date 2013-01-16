package azkaban.webapp.servlet;

public class ViewerPlugin {
	private final String pluginName;
	private final String pluginPath;
	
	public ViewerPlugin(String pluginName, String pluginPath) {
		this.pluginName = pluginName;
		this.pluginPath = pluginPath;
	}

	public String getPluginName() {
		return pluginName;
	}

	public String getPluginPath() {
		return pluginPath;
	}
}
