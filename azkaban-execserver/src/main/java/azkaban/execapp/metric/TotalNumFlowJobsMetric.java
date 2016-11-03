package azkaban.execapp.metric;

import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.execapp.FlowRunner;
import azkaban.execapp.JobRunner;
import azkaban.metric.AbstractMetric;
import azkaban.metric.MetricReportManager;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metric for the total number of jobs(nodes) that ran for a flow
 */
public class TotalNumFlowJobsMetric extends AbstractMetric<Integer> implements EventListener {
    public static final String TOTAL_NUM_FLOW_JOBS_METRIC_NAME = "TotalNumFlowJobs";
    private static final String TOTAL_NUM_FLOW_JOBS_METRIC_TYPE = "uint16";
    protected ConcurrentHashMap<String, Integer> flowsWithJobs = new ConcurrentHashMap<>();

    /**
     * @param manager      Metric Manager whom a metric will report to
     */
    public TotalNumFlowJobsMetric(MetricReportManager manager) {
        super(TOTAL_NUM_FLOW_JOBS_METRIC_NAME, TOTAL_NUM_FLOW_JOBS_METRIC_TYPE, 0, manager);
    }

    /**
     * Listens for events and keeps track of the jobs started for each flow currently executing
     * then reports the count when the flow has finished.
     * @param event         Event which has fired to listeners
     */
    @Override
    public void handleEvent(Event event) {
        if (event.getType() == Event.Type.JOB_STARTED) {
            JobRunner jobRunner = (JobRunner) event.getRunner();
            if (jobRunner != null) {
                String flowId = "flow:" + jobRunner.getNode().getParentFlow().getFlowId();
                flowsWithJobs.compute(flowId, (k, v) -> v == null ? 1 : v + 1);
            }
        } else if (event.getType() == Event.Type.FLOW_FINISHED) {
            FlowRunner flowRunner = (FlowRunner) event.getRunner();
            if (flowRunner != null) {
                String flowId = "flow:" + flowRunner.getExecutableFlow().getFlowId();
                flowsWithJobs.putIfAbsent(flowId, 0);
                value = flowsWithJobs.get(flowId);
                tags = Arrays.asList(flowId);
                notifyManager();
                postTrackingEventHandler(flowId);
            }
        }
    }

    /**
     * Removes the count of jobs for the flow that we have reported, to stop tracking its jobs.
     *
     * @param flowId   The id of the flow that we just reported
     */
    private void postTrackingEventHandler(String flowId) {
        value = 0;
        flowsWithJobs.remove(flowId);
    }

}
