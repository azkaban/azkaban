package azkaban.execapp;

import azkaban.executor.ExecutableNode;

public interface FlowBlocker {
	public boolean readyToRun(ExecutableNode node);
}
