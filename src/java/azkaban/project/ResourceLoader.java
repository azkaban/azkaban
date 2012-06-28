package azkaban.project;

import azkaban.utils.Props;

public interface ResourceLoader {
	public Props loadPropsFromSource(String source);
	
	public Props loadPropsFromSource(Props parent, String source);
}
