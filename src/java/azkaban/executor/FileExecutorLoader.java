package azkaban.executor;

public class FileExecutorLoader implements ExecutorLoader {
	@Override
	public String getUniqueExecutionId() {
		return "";
	}
	
	@Override
	public void commitExecutableFlow(ExecutableFlow flow) {
		
	}
	
	@Override
	public ExecutableFlow loadExecutableFlow(String executionId) {
		return null;
	}
}
