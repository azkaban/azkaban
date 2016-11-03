package azkaban.execapp.metric;

import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.execapp.FlowRunner;
import azkaban.metric.AbstractMetric;
import azkaban.metric.MetricReportManager;

import java.util.Arrays;

/**
 * Metric for the time a flow took to execute all jobs in it
 */
public class FlowDurationMetric extends AbstractMetric<Integer> implements EventListener {
    public static final String FLOW_EXECUTION_DURATION_METRIC_NAME = "FlowExecutionDuration";
    private static final String FLOW_EXECUTION_DURATION_METRIC_TYPE = "uint16";

    /**
     * @param manager      Metric Manager whom a metric will report to
     */
    public FlowDurationMetric(MetricReportManager manager) {
        super(FLOW_EXECUTION_DURATION_METRIC_NAME, FLOW_EXECUTION_DURATION_METRIC_TYPE, 0, manager);
    }

    /**
     * Listen for events and compute time a flow took to execute and report to Metrics Manager
     * @param event         An event which have been reported to listeners
     */
    @Override
    public void handleEvent(Event event) {
        if (event.getType() == Event.Type.FLOW_FINISHED) {
            FlowRunner runner = (FlowRunner) event.getRunner();
            if (runner != null) {
                String flowName = "flow:" + runner.getExecutableFlow().getFlowId();
                String projectName = "project:" + runner.getExecutableFlow().getProjectName();
                // Calculate the time the flow took to execute
                value = (int) ((runner.getExecutableFlow().getEndTime() - runner.getExecutableFlow().getStartTime()) / 1000);
                tags = Arrays.asList(flowName, projectName);
                notifyManager();
            }
        }
    }
}
