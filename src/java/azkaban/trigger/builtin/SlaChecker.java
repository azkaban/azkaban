package azkaban.trigger.builtin;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.sla.SlaOption;
import azkaban.trigger.ConditionChecker;
import azkaban.utils.Utils;

public class SlaChecker implements ConditionChecker{

	private static final Logger logger = Logger.getLogger(SlaChecker.class);
	public static final String type = "SlaChecker";
	
	private String id;
	private SlaOption slaOption;
	private int execId;
	private long checkTime = -1;
	
	private static ExecutorManagerAdapter executorManager;
	
	public SlaChecker(String id, SlaOption slaOption, int execId) {
		this.id = id;
		this.slaOption = slaOption;
		this.execId = execId;
	}

	public static void setExecutorManager(ExecutorManagerAdapter em) {
		executorManager = em;
	}
	
	private Boolean violateSla(ExecutableFlow flow) {
		String type = slaOption.getType();
		logger.info("Checking for " + flow.getExecutionId() + " with sla " + type);
		logger.info("flow is " + flow.getStatus());
		if(flow.getStartTime() < 0) {
			return Boolean.FALSE;
		}
		if(type.equals(SlaOption.TYPE_FLOW_FINISH)) {
			ReadablePeriod duration = Utils.parsePeriodString((String) slaOption.getInfo().get(SlaOption.INFO_DURATION));
			DateTime startTime = new DateTime(flow.getStartTime());
			DateTime checkTime = startTime.plus(duration);
			this.checkTime = checkTime.getMillis();
			if(checkTime.isBeforeNow()) {
				Status status = flow.getStatus();
				if(status.equals(Status.FAILED) || status.equals(Status.KILLED) || status.equals(Status.SUCCEEDED)) {
					return Boolean.FALSE;
				} else {
					return Boolean.TRUE;
				}
			}
		} else if(type.equals(SlaOption.TYPE_FLOW_SUCCEED)) {
			ReadablePeriod duration = Utils.parsePeriodString((String) slaOption.getInfo().get(SlaOption.INFO_DURATION));
			DateTime startTime = new DateTime(flow.getStartTime());
			DateTime checkTime = startTime.plus(duration);
			this.checkTime = checkTime.getMillis();
			if(checkTime.isBeforeNow()) {
				Status status = flow.getStatus();
				if(status.equals(Status.SUCCEEDED)) {
					return Boolean.FALSE;
				} else {
					return Boolean.TRUE;
				}
			}
		} else if(type.equals(SlaOption.TYPE_JOB_FINISH)) {
			String jobName = (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME); 
			ExecutableNode node = flow.getExecutableNode(jobName);
			if(node.getStartTime() > 0) {
				ReadablePeriod duration = Utils.parsePeriodString((String) slaOption.getInfo().get(SlaOption.INFO_DURATION));
				DateTime startTime = new DateTime(node.getStartTime());
				DateTime checkTime = startTime.plus(duration);
				this.checkTime = checkTime.getMillis();
				if(checkTime.isBeforeNow()) {
					Status status = node.getStatus();
					if(status.equals(Status.FAILED) || status.equals(Status.KILLED) || status.equals(Status.SUCCEEDED)) {
						return Boolean.FALSE;
					} else {
						return Boolean.TRUE;
					}
				}
			}
		} else if(type.equals(SlaOption.TYPE_JOB_SUCCEED)) {
			String jobName = (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME); 
			ExecutableNode node = flow.getExecutableNode(jobName);
			if(node.getStartTime() > 0) {
				ReadablePeriod duration = Utils.parsePeriodString((String) slaOption.getInfo().get(SlaOption.INFO_DURATION));
				DateTime startTime = new DateTime(node.getStartTime());
				DateTime checkTime = startTime.plus(duration);
				this.checkTime = checkTime.getMillis();
				if(checkTime.isBeforeNow()) {
					Status status = node.getStatus();
					if(status.equals(Status.SUCCEEDED)) {
						return Boolean.FALSE;
					} else {
						return Boolean.TRUE;
					}
				}
			}
		} 
//		else if(type.equals(SlaOption.TYPE_JOB_PROGRESS)) {
//			String jobName = (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME); 
//			float targetProgress = Float.valueOf((String) slaOption.getInfo().get(SlaOption.INFO_PROGRESS_PERCENT));
//			ExecutableNode node = flow.getExecutableNode(jobName);
//			if(node.getStartTime() > 0) {
//				ReadablePeriod duration = Utils.parsePeriodString((String) slaOption.getInfo().get(SlaOption.INFO_DURATION));
//				DateTime startTime = new DateTime(node.getStartTime());
//				DateTime checkTime = startTime.plus(duration);
//				if(checkTime.isBeforeNow()) {
//					if(node.getProgress() > targetProgress) {
//						return Boolean.FALSE;
//					} else {
//						return Boolean.TRUE;
//					}
//				}
//			} else {
//				return Boolean.FALSE;
//			}
//		}
		return Boolean.FALSE;
	}
	
	// return true to trigger sla action
	@Override
	public Object eval() {
		ExecutableFlow flow;
		try {
			flow = executorManager.getExecutableFlow(execId);
		} catch (ExecutorManagerException e) {
			logger.error("Can't get executable flow.", e);
			e.printStackTrace();
			// something wrong, send out alerts
			return Boolean.TRUE;
		}
		return violateSla(flow);
	}

	@Override
	public Object getNum() {
		return null;
	}

	@Override
	public void reset() {
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public String getType() {
		return type;
	}

	@Override
	public ConditionChecker fromJson(Object obj) throws Exception {
		return createFromJson(obj);
	}

	@SuppressWarnings("unchecked")
	public static SlaChecker createFromJson(Object obj) throws Exception {
		return createFromJson((HashMap<String, Object>)obj);
	}
	
	public static SlaChecker createFromJson(HashMap<String, Object> obj) throws Exception {
		Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
		if(!jsonObj.get("type").equals(type)) {
			throw new Exception("Cannot create checker of " + type + " from " + jsonObj.get("type"));
		}
		String id = (String) jsonObj.get("id");
		SlaOption slaOption = SlaOption.fromObject(jsonObj.get("slaOption"));
		int execId = Integer.valueOf((String) jsonObj.get("execId"));
		return new SlaChecker(id, slaOption, execId);
	}
	
	@Override
	public Object toJson() {
		Map<String, Object> jsonObj = new HashMap<String, Object>();
		jsonObj.put("type", type);
		jsonObj.put("id", id);
		jsonObj.put("slaOption", slaOption.toObject());
		jsonObj.put("execId", String.valueOf(execId));
	
		return jsonObj;
	}

	@Override
	public void stopChecker() {
		
	}

	@Override
	public void setContext(Map<String, Object> context) {
	}

	@Override
	public long getNextCheckTime() {
		return checkTime;
	}

}
