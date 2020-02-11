package cloudflow.models;

import azkaban.executor.ExecutableFlow;
import com.sun.istack.internal.NotNull;

public class ExecutionDetailedResponse extends ExecutionBasicResponse {

  @NotNull
  private final ExecutionNodeResponse rootExecutableNode;

  public ExecutionDetailedResponse(ExecutableFlow executableFlow,
      ExecutionNodeResponse rootExecutableNode) {
    super(executableFlow);
    this.rootExecutableNode = rootExecutableNode;
  }

  public ExecutionNodeResponse getRootExecutableNode() {
    return rootExecutableNode;
  }
}
