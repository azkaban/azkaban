package azkaban.test.executor;

import java.util.Map;

public class SleepJavaJob {
	private boolean fail;
	private String seconds;
	@SuppressWarnings("unchecked")
	public SleepJavaJob(String id, Map<String, String> parameters) {
		String failStr = parameters.get("fail");
		if (failStr == null || failStr.equals("false")) {
			fail = false;
		}
		else {
			fail = true;
		}
	
		seconds = parameters.get("seconds");
		System.out.println("Properly created");
	}
	
	public void run() throws Exception {
		if (seconds == null) {
			throw new RuntimeException("Seconds not set");
		}
		
		int sec = Integer.parseInt(seconds);
		System.out.println("Sec " + sec);
		synchronized(this) {
			try {
				this.wait(sec*1000);
			} catch (InterruptedException e) {
				System.out.println("Interrupted " + fail);
			}
		}
		
		if (fail) {
			throw new Exception("I failed because I had to.");
		}
	}
	
	public void cancel() throws Exception {
		System.out.println("Cancelled called on Sleep job");
		fail = true;
		synchronized(this) {
			this.notifyAll();
		}
	}
}
