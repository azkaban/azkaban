package azkaban.project;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import azkaban.utils.Pair;
import azkaban.utils.Props;

public class FileResourceLoader implements ResourceLoader {
	private HashMap<Pair<String, String>, Props> propsCache = new HashMap<Pair<String,String>, Props>();
	private File basePath;
	
	public FileResourceLoader(File basePath) {
		this.basePath = basePath;
	}
	
	@Override
	public Props loadPropsFromSource(String source) {
		return loadPropsFromSource(null, source);
	}
	
	@Override
	public Props loadPropsFromSource(Props parent, String source) {
		String parentSource = parent == null ? "null" : parent.getSource();
		Pair<String, String> pair = new Pair<String,String>(parentSource, source);
		Props props = propsCache.get(pair);
		if (props != null) {
			return props;
		}

		File path = new File(basePath, source);

		if (!path.exists()) {
			props = createErrorProps("Source file " + source + " doesn't exist.");
		}
		else if (!path.isFile()) {
			props = createErrorProps("Source file " + source + " isn't a file.");
		}
		else {
			try {
				props = new Props(parent, path);
			} catch (IOException e) {
				props = createErrorProps("Error loading resource: " + e.getMessage());
			}
		}
		
		propsCache.put(pair, props);
		return props;
	}

	private Props createErrorProps(String message) {
		Props props = new Props();
		props.put("error", message);
		return props;
	}
}
