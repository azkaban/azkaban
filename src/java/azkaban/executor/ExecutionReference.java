package azkaban.executor;

import java.util.HashMap;

public class ExecutionReference {
	private final int execId;
	private final String host;
	private final int port;
	private long updateTime;
	private long nextCheckTime = -1;
	private int numErrors = 0;
	
	public ExecutionReference(int execId, String host, int port) {
		this.execId = execId;
		this.host = host;
		this.port = port;
	}
	
	public void setUpdateTime(long updateTime) {
		this.updateTime = updateTime;
	}
	
	public void setNextCheckTime(long nextCheckTime) {
		this.nextCheckTime = nextCheckTime;
	}

	public long getUpdateTime() {
		return updateTime;
	}

	public long getNextCheckTime() {
		return nextCheckTime;
	}

	public int getExecId() {
		return execId;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public int getNumErrors() {
		return numErrors;
	}

	public void setNumErrors(int numErrors) {
		this.numErrors = numErrors;
	}
 }