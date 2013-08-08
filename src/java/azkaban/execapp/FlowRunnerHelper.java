package azkaban.execapp;

import java.util.ArrayList;
import java.util.List;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowBase;
import azkaban.executor.ExecutableNode;
import azkaban.executor.Status;

public class FlowRunnerHelper {
	public static boolean isFlowFinished(ExecutableFlow flow) {
		for (String end: flow.getEndNodes()) {
			ExecutableNode node = flow.getExecutableNode(end);
			if (!Status.isStatusFinished(node.getStatus()) ) {
				return false;
			}
		}
		
		return true;
	}
	
	public static List<ExecutableNode> findReadyJobsToRun(ExecutableFlowBase flow) {
		ArrayList<ExecutableNode> jobsToRun = new ArrayList<ExecutableNode>();
		for (ExecutableNode node : flow.getExecutableNodes()) {
			if (Status.isStatusFinished(node.getStatus())) {
				continue;
			}
			else {
				// Check the dependencies to see if execution conditions are met,
				// and what the status should be set to.
				Status impliedStatus = getImpliedStatus(node);
				if (impliedStatus != null) {
					node.setStatus(impliedStatus);
					jobsToRun.add(node);
				}
				else if (node instanceof ExecutableFlowBase && node.getStatus() == Status.RUNNING) {
					// We want to seek into a running flow
				}
			}
		}
		
		return jobsToRun;
	}
	
	public static Status getImpliedStatus(ExecutableNode node) {
		switch(node.getStatus()) {
			case FAILED:
			case KILLED:
			case SKIPPED:
			case SUCCEEDED:
			case FAILED_SUCCEEDED:
			case QUEUED:
			case RUNNING:
				return null;
			default:
				break;
		}
		
		ExecutableFlowBase flow = node.getParentFlow();
		
		boolean shouldKill = false;
		for (String dependency : node.getInNodes()) {
			ExecutableNode dependencyNode = flow.getExecutableNode(dependency);
			
			Status depStatus = dependencyNode.getStatus();
			switch (depStatus) {
			case FAILED:
			case KILLED:
				shouldKill = true;
			case SKIPPED:
			case SUCCEEDED:
			case FAILED_SUCCEEDED:
				continue;
			case RUNNING:
			case QUEUED:
			case DISABLED:
				return null;
			default:
				// Return null means it's not ready to run.
				return null;
			}
		}
		
		if (shouldKill || flow.getStatus() == Status.KILLED || flow.getStatus() == Status.FAILED) {
			return Status.KILLED;
		}
		
		// If it's disabled but ready to run, we want to make sure it continues being disabled.
		if (node.getStatus() == Status.DISABLED) {
			return Status.DISABLED;
		}
		
		// All good to go, ready to run.
		return Status.READY;
	}
}
