package azkaban.executor;

public interface ExecutorLoader {
	public String getUniqueExecutionId();
	
	public void commitExecutableFlow(ExecutableFlow exflow);
	
	public ExecutableFlow loadExecutableFlow(String executionId);
}
