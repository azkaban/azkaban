package azkaban.test.executor;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.utils.Props;

public class InteractiveTestJob extends AbstractProcessJob {
	private static ConcurrentHashMap<String, InteractiveTestJob> testJobs = new ConcurrentHashMap<String, InteractiveTestJob>();
	private Props generatedProperties = new Props();
	private boolean isWaiting = true;
	private boolean succeed = true;

	public static InteractiveTestJob getTestJob(String name) {
		return testJobs.get(name);
	}
	
	public static void clearTestJobs() {
		testJobs.clear();
	}
	
	public InteractiveTestJob(String jobId, Props sysProps, Props jobProps, Logger log) {
		super(jobId, sysProps, jobProps, log);
	}

	@Override
	public void run() throws Exception {
		String nestedFlowPath = this.getJobProps().get(CommonJobProperties.NESTED_FLOW_PATH);
		String groupName = this.getJobProps().getString("group", null);
		String id = nestedFlowPath == null ? this.getId() : nestedFlowPath;
		if (groupName != null) {
			id = groupName + ":" + id;
		}
		testJobs.put(id, this);
		
		while(isWaiting) {
			synchronized(this) {
				try {
					wait(30000);
				} catch (InterruptedException e) {
				}
				
				if (!isWaiting) {
					if (!succeed) {
						throw new RuntimeException("Forced failure of " + getId());
					}
					else {
						info("Job " + getId() + " succeeded.");
					}
				}
			}
		}
	}
	
	public void failJob() {
		synchronized(this) {
			succeed = false;
			isWaiting = false;
			this.notify();
		}
	}
	
	public void succeedJob() {
		synchronized(this) {
			succeed = true;
			isWaiting = false;
			this.notify();
		}
	}
	
	public void succeedJob(Props generatedProperties) {
		synchronized(this) {
			this.generatedProperties = generatedProperties;
			succeed = true;
			isWaiting = false;
			this.notify();
		}
	}
	
	@Override
	public Props getJobGeneratedProperties() {
		return generatedProperties;
	}

	@Override
	public void cancel() throws InterruptedException {
		info("Killing job");
		failJob();
	}
}
