package azkaban.executor;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import azkaban.utils.Props;

public class ExecutorManager {
	private static Logger logger = Logger.getLogger(ExecutorManager.class);
	private AtomicLong counter = new AtomicLong();
	private String token;
	
	private HashMap<String, ExecutableFlow> runningFlows = new HashMap<String, ExecutableFlow>();
	
	public ExecutorManager(Props props) {
		token = props.getString("executor.shared.token", "");
		counter.set(0);
	}
	
	public void executeFlow(ExecutableFlow flow) {
		
	}
	
	private class ExecutingFlow implements Runnable {
		public void run() {
			
		}
	}
}
