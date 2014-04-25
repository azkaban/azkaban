package azkaban.migration.schedule2trigger;

public class CommonParams {
	public static final String TYPE_FLOW_FINISH = "FlowFinish";
	public static final String TYPE_FLOW_SUCCEED = "FlowSucceed";
	public static final String TYPE_FLOW_PROGRESS = "FlowProgress";

	public static final String TYPE_JOB_FINISH = "JobFinish";
	public static final String TYPE_JOB_SUCCEED = "JobSucceed";
	public static final String TYPE_JOB_PROGRESS = "JobProgress";

	public static final String INFO_DURATION = "Duration";
	public static final String INFO_FLOW_NAME = "FlowName";
	public static final String INFO_JOB_NAME = "JobName";
	public static final String INFO_PROGRESS_PERCENT = "ProgressPercent";
	public static final String INFO_EMAIL_LIST = "EmailList";

	// always alert
	public static final String ALERT_TYPE = "SlaAlertType";
	public static final String ACTION_CANCEL_FLOW = "SlaCancelFlow";
	public static final String ACTION_ALERT = "SlaAlert";
}
