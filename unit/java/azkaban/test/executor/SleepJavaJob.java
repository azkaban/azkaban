package azkaban.test.executor;

import java.util.Map;

import azkaban.utils.Props;

public class SleepJavaJob {
	private Props props;
	@SuppressWarnings("unchecked")
	public SleepJavaJob(String id, Map<String, String> parameters) {
		props = new Props(null, parameters);

		System.out.println("Properly created");
	}
	
	public void run() throws Exception {
		int sec = props.getInt("seconds");
		boolean fail = props.getBoolean("fail", false);
		synchronized(this) {
			try {
				this.wait(sec*1000);
			} catch (InterruptedException e) {
				System.out.println("Interrupted");
			}
		}
		
		if (fail) {
			throw new Exception("I failed because I had to.");
		}
	}
	
	public void cancel() throws Exception {
		System.out.println("Cancelled called");
		this.notifyAll();
	}
}
