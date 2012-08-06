package azkaban.executor;

import java.util.HashMap;

import azkaban.flow.Flow;
import azkaban.utils.Props;

public class ExecutableFlow {
	private Flow flow;
	private HashMap<String, Props> sourceProps = new HashMap<String, Props>();
	
	public ExecutableFlow(Flow flow, HashMap<String,Props> sourceProps) {
		this.flow = flow;
		this.sourceProps = sourceProps;
	}
	
	public void run() {
		
	}
}
