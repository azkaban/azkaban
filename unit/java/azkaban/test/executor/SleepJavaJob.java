package azkaban.test.executor;

import java.io.File;
import java.io.FileFilter;
import java.util.Map;

public class SleepJavaJob {
	private boolean fail;
	private String seconds;
	private int attempts;
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
	
		String attemptString = parameters.get("passRetry");
		if (attemptString == null) {
			attempts = -1;
		}
		else {
			attempts = Integer.valueOf(attemptString);
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
		
		File file = new File("");
		File[] attemptFiles = file.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.getName().startsWith(id);
			}});
		
		if (fail) {
			if (attempts <= 0 || attemptFiles == null || attemptFiles.length > attempts) {
				File attemptFile = new File(file, id + "." + (attemptFiles == null ? 0 : attemptFiles.length));

				attemptFile.mkdirs();
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
