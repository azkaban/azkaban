package azkaban.trigger.builtin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorManager;
import azkaban.executor.Status;
import azkaban.flow.CommonJobProperties;
import azkaban.sla.SlaOption;
import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.triggerapp.TriggerRunnerManager;
import azkaban.utils.Utils;

public class WatchSlaAction implements TriggerAction{

	private static final Logger logger = Logger.getLogger(WatchSlaAction.class);
	public static final String type = "SetSlaWatcherAction";
	
	private Map<String, Object> context;
	private List<SlaOption> slaOptions;
	private String actionId;
	private String executionActionId;
	private static ExecutorManager executorManager;
	private static TriggerRunnerManager trigggerRunnerManager;
	
	public WatchSlaAction(String actionId, List<SlaOption> slaOptions, String executionActionId) {
		this.actionId = actionId;
		this.slaOptions = slaOptions;
		this.executionActionId = executionActionId;
	}
	
	public static void setExecutorManager(ExecutorManager em) {
		executorManager = em;
	}
	
	public static void setTriggerRunnerManager(TriggerRunnerManager trm) {
		trigggerRunnerManager = trm;
	}
	
	@Override
	public String getId() {
		return actionId;
	}

	@Override
	public String getType() {
		return type;
	}

	@SuppressWarnings("unchecked")
	public static WatchSlaAction createFromJson(Object obj) {
		Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
		String objType = (String) jsonObj.get("type");
		if(! objType.equals(type)) {
			throw new RuntimeException("Cannot create action of " + type + " from " + objType);
		}
		String actionId = (String) jsonObj.get("actionId");
		String executionActionId = (String) jsonObj.get("executionActionId");
		List<SlaOption> slaOptions = new ArrayList<SlaOption>();
		List<Object> slaOptionsObj = (List<Object>) jsonObj.get("slaOptions");
		for(Object slaObj : slaOptionsObj) {
			slaOptions.add(SlaOption.fromObject(slaObj));
		}
		return new WatchSlaAction(actionId, slaOptions, executionActionId);

	}
	
	@Override
	public TriggerAction fromJson(Object obj) throws Exception {
		return createFromJson(obj);
	}

	@Override
	public Object toJson() {
		Map<String, Object> jsonObj = new HashMap<String, Object>();
		jsonObj.put("actionId", actionId);
		jsonObj.put("type", type);
		jsonObj.put("executionActionId", executionActionId);
		List<Object> slaOptionsObj = new ArrayList<Object>();
		for(SlaOption sla : slaOptions) {
			slaOptionsObj.add(sla.toObject());
		}
		jsonObj.put("slaOptions", slaOptionsObj);

		return jsonObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void doAction() throws Exception {
		
		if(executorManager == null) {
			throw new Exception("ExecutorManager not initialized. Failed to set sla.");
		}
		if(trigggerRunnerManager == null) {
			throw new Exception("TriggerRunnerManager not initialized. Failed to set sla.");
		}
		
		if(!context.containsKey(executionActionId)) {
			throw new Exception("No trace of the execution. Cannot set sla if the flow/job is not run.");
		}
		Map<String, Object> executionInfo = (Map<String, Object>) context.get(executionActionId);
		if(!executionInfo.containsKey(ExecuteFlowAction.EXEC_ID)) {
			throw new Exception("Execution Id not set. Failed to set sla.");
		}
		int execId = Integer.valueOf((String) executionInfo.get(ExecuteFlowAction.EXEC_ID));
		ExecutableFlow exflow = executorManager.getExecutableFlow(execId);
		
		for(SlaOption sla : slaOptions) {
			if(sla.getType().equals(SlaOption.TYPE_FLOW_FINISH)) {
			// just do a time checker and see if the flow finish
				ConditionChecker timer = createFlowSlaTimer(exflow, sla);
				ExecutionChecker statusChecker = new ExecutionChecker("slaStatusChecker1", execId, ExecutionChecker.TARGET_FINISHED, null);
				String failExpr = timer.getId() + ".eval() && !" + statusChecker.getId() + ".eval()";
				Map<String, ConditionChecker> checkers = new HashMap<String, ConditionChecker>();
				checkers.put(timer.getId(), timer);
				checkers.put(statusChecker.getId(), statusChecker);
				Condition triggerCondition = new Condition(checkers, failExpr);
				List<String> slaActions = sla.getActions();
				List<TriggerAction> actions = new ArrayList<TriggerAction>();
				// always send email
				SendEmailAction emailAct = new SendEmailAction("sendSlaAlertEmail", getSlaEmailSubject(exflow), getSlaEmailMessage(exflow, sla), (List<String>) sla.getInfo().get("emailList"));
				actions.add(emailAct);
				for(String act : slaActions) {
					if(act.equals(SlaOption.ACTION_CANCEL_FLOW)) {
						KillExecutionAction killAct = new KillExecutionAction("slaKiller1", exflow.getExecutionId());
						actions.add(killAct);
					} 
//					else if(act.equals(SlaOption.ACTION_ALERT_BY_EMAIL)) {
//						SendEmailAction emailAct = new SendEmailAction("sendSlaAlertEmail", getSlaEmailSubject(exflow), getSlaEmailMessage(exflow, sla), (List<String>) sla.getInfo().get("emailList"));
//						actions.add(emailAct);
//					}
				}
				
				Trigger t = new Trigger("azkaban", "triggerserver", triggerCondition, triggerCondition, actions);
				t.setResetOnTrigger(false);
				t.setResetOnExpire(false);
				trigggerRunnerManager.insertTrigger(t);
			} else if(sla.getType().equals(SlaOption.TYPE_FLOW_SUCCEED)) {
//				String jobName = (String) sla.getInfo().get("jobName");
//				ExecutableNode exNode = exflow.getExecutableNode(jobName);
//				ConditionChecker timer = createJobSlaTimer(exNode, sla);
//				if(exNode.getStatus().equals(Status.RUNNING)) {
//					
//				}
			} else {
				logger.error("Unknown sla type.");
			}
		}
	}
	
	private BasicTimeChecker createFlowSlaTimer(ExecutableFlow exflow, SlaOption sla) {
		Map<String, Object> info = sla.getInfo();
		ReadablePeriod duration = Utils.parsePeriodString((String) info.get(SlaOption.INFO_DURATION));
		DateTime startTime = new DateTime(exflow.getSubmitTime());
		DateTime checkTime = startTime.plus(duration);
		BasicTimeChecker timeChecker = new BasicTimeChecker("slaTimer1", checkTime, checkTime.getZone(), false, false, null);
		return timeChecker;
	}
	
//	private BasicTimeChecker createJobSlaTimer(ExecutableNode exNode, SlaOption sla) {
//		Map<String, Object> info = sla.getInfo();
//		ReadablePeriod runtimelimit = Utils.parsePeriodString((String) info.get(SlaOption.INFO_RUNTIMELIMIT));
//		DateTime startTime = new DateTime(exNode.getStartTime());
//		DateTime checkTime = startTime.plus(runtimelimit);
//		BasicTimeChecker timeChecker = new BasicTimeChecker("slaTimer1", checkTime, checkTime.getZone(), false, false, null);
//		return timeChecker;
//	} 

	private String getSlaEmailSubject(ExecutableFlow flow) {
		return "A preset SLA on flow " + flow.getFlowId() + " is not met in execution " + flow.getExecutionId();
	}
	
	private String getSlaEmailMessage(ExecutableFlow flow, SlaOption slaOption) {
		if(slaOption.getType().equals(SlaOption.TYPE_FLOW_FINISH)) {
			return "<h2 style=\"color:#FF0000\"> Execution '" + flow.getExecutionId() + "' of flow '" + flow.getFlowId() + "' has exceeded its expected finish time'" + "</h2>";
		}
		if(slaOption.getType().equals(SlaOption.TYPE_FLOW_SUCCEED)) {
			return "<h2 style=\"color:#FF0000\"> Execution '" + flow.getExecutionId() + "' of flow '" + flow.getFlowId() + "' has exceeded its expected time to succeed'" + "</h2>";
		}
		return "Unrecognized SLA type.";
	}
	
	@Override
	public void setContext(Map<String, Object> context) {
		this.context = context;
	}

	@Override
	public String getDescription() {
		return type;
	}
	
}
