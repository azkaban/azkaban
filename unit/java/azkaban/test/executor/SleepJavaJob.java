package azkaban.test.executor;

import java.io.File;
import java.io.FileFilter;
import java.util.Map;

public class SleepJavaJob {
	private boolean fail;
	private String seconds;
	private int attempts;
	private int currentAttempt;
	private String id;

	public SleepJavaJob(String id, Map<String, String> parameters) {
		this.id = id;
		String failStr = parameters.get("fail");
		
		if (failStr == null || failStr.equals("false")) {
			fail = false;
		}
		else {
			fail = true;
		}
	
		currentAttempt = parameters.containsKey("azkaban.job.attempt") ? Integer.parseInt(parameters.get("azkaban.job.attempt")) : 0;
		String attemptString = parameters.get("passRetry");
		if (attemptString == null) {
			attempts = -1;
		}
		else {
			attempts = Integer.valueOf(attemptString);
		}
		seconds = parameters.get("seconds");

		if (fail) {
			System.out.println("Planning to fail after " + seconds + " seconds. Attempts left " + currentAttempt + " of " + attempts);
		}
		else {
			System.out.println("Planning to succeed after " + seconds + " seconds.");
		}
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
			if (attempts <= 0 || currentAttempt <= attempts) {
				throw new Exception("I failed because I had to.");
			}
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
