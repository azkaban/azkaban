package azkaban.execapp.event;

import azkaban.executor.Status;

public class BlockingStatus {
	private static final long WAIT_TIME = 5*60*1000;
	private final int execId;
	private final String jobId;
	private Status status;
	
	public BlockingStatus(int execId, String jobId, Status initialStatus) {
		this.execId = execId;
		this.jobId = jobId;
		this.status = initialStatus;
	}
	
	public Status blockOnFinishedStatus() {
		if (status == null) {
			return null;
		}
		
		while (!Status.isStatusFinished(status)) {
			synchronized(this) {
				try {
					this.wait(WAIT_TIME);
				} catch (InterruptedException e) {
				}
			}
		}
		
		return status;
	}

	public Status viewStatus() {
		return this.status;
	}
	
	public void unblock() {
		synchronized(this) {
			this.notifyAll();
		}
	}
	
	public void changeStatus(Status status) {
		synchronized(this) {
			this.status = status;
			if (Status.isStatusFinished(status)) {
				unblock();
			}
		}
	}
	
	public int getExecId() {
		return execId;
	}

	public String getJobId() {
		return jobId;
	}
}
