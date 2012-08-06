package azkaban.executor;

import azkaban.utils.Props;

public class ExecutorManager {
	private String token;
	
	public ExecutorManager(Props props) {
		token = props.getString("executor.shared.token", "");
	}
	
	public void executeJob() {
		
	}
	
	private class ExecutorThread extends Thread {
		public void run() {
			
		}
	}
}
