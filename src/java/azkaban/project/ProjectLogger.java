package azkaban.project;

import java.io.File;
import java.util.logging.Level;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

/*
 * 
 */
public class ProjectLogger {
	
	private static final Layout DEFAULT_LAYOUT = new PatternLayout("%d{dd-MM-yyyy HH:mm:ss z} %c{1} %p - %m\n");
	private static final int TIME_TO_IDLE_SECS = 30 * 60; // 30 mins
	private static final String PROJECT_SUFFIX = ".PROJECT";
	private CacheManager manager = CacheManager.create();
	private Cache cache;
	private String path;

	public ProjectLogger(String projectPath) {
		CacheConfiguration config = new CacheConfiguration();
		config.setName("loggerCache");
		config.setTimeToLiveSeconds(TIME_TO_IDLE_SECS);
		config.eternal(false);
		config.diskPersistent(false);
		config.memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU);

		cache = new Cache(config);
		manager.addCache(cache);
	}

	public void log(Level level, String project, String message) {

	}

	private Logger getLogger(String projectId) {
		Element element = cache.get(projectId);
		Logger logger = Logger.getLogger(projectId + PROJECT_SUFFIX);

//		if (element == null) {
//			File file = new File(path);
//			new File(file, );
//			Appender appender = new FileAppender(DEFAULT_LAYOUT, , false);
//			logger.addAppender(jobAppender);
//		}
//		else {
//			Object obj = element.getObjectValue();
//		}

		return null;
	}
}
