package azkaban.executor;

import azkaban.utils.Pair;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Singleton;

@Singleton
public class RunningExecutions {

  final ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>> runningFlows =
      new ConcurrentHashMap<>();

  public Pair<ExecutionReference, ExecutableFlow> get(final int executionId) {
    return this.runningFlows.get(executionId);
  }

  public int size() {
    return this.runningFlows.size();
  }

  public Collection<Pair<ExecutionReference, ExecutableFlow>> values() {
    return this.runningFlows.values();
  }

  public void putAll(final Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows) {
    this.runningFlows.putAll(activeFlows);
  }

  public boolean containsKey(final int executionId) {
    return this.runningFlows.containsKey(executionId);
  }

  public void remove(final int execId) {
    this.runningFlows.remove(execId);
  }

  public void put(final int executionId,
      final Pair<ExecutionReference, ExecutableFlow> executionReferenceExecutableFlowPair) {
    this.runningFlows.put(executionId, executionReferenceExecutableFlowPair);
  }

}
