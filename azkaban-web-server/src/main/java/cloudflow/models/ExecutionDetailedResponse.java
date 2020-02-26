package cloudflow.models;

import azkaban.executor.ExecutableFlow;
import javax.annotation.Nonnull;
import org.codehaus.jackson.annotate.JsonProperty;

public class ExecutionDetailedResponse extends ExecutionBasicResponse {

  @JsonProperty("rootFlow")
  @Nonnull
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
