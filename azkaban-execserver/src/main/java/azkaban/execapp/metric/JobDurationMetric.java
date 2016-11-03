package azkaban.execapp.metric;

import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.execapp.JobRunner;
import azkaban.metric.AbstractMetric;
import azkaban.metric.MetricReportManager;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JobDurationMetric extends AbstractMetric<Integer> implements EventListener {
    public static final String JOB_DURATION_METRIC_NAME = "JobExecutionDuration";
    private static final String JOB_DURATION_METRIC_TYPE = "uint16";

    /**
     * @param manager      Metric Manager whom the metric will report to
     */
    public JobDurationMetric(MetricReportManager manager) {
        super(JOB_DURATION_METRIC_NAME, JOB_DURATION_METRIC_TYPE, 0, manager);
    }

    /**
     * Listens for events and reports the duration of finished events
     * @param event     Event which has been reported
     */
    @Override
    public void handleEvent(Event event) {
        if (event.getType() == Event.Type.JOB_FINISHED) {
            JobRunner jobRunner = (JobRunner) event.getRunner();
            if (jobRunner != null) {
                String jobName = "job:" + jobRunner.getNode().getId();
                String flowName = "flow:" + jobRunner.getNode().getParentFlow().getFlowId();
                Integer jobDuration = (int) ((jobRunner.getNode().getEndTime() - jobRunner.getNode().getStartTime()) / 1000);
                tags = Arrays.asList(jobName, flowName);
                value = jobDuration;
                notifyManager();
            }
        }
    }
}
