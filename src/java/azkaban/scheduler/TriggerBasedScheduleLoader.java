package azkaban.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import azkaban.actions.ExecuteFlowAction;
import azkaban.executor.ExecutorManager;
import azkaban.project.ProjectManager;
import azkaban.trigger.Condition;
import azkaban.trigger.ConditionChecker;
import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.TriggerManager;
import azkaban.trigger.TriggerManagerException;
import azkaban.trigger.TriggerStatus;

public class TriggerBasedScheduleLoader implements ScheduleLoader {
	
	private static Logger logger = Logger.getLogger(TriggerBasedScheduleLoader.class);
	
	private TriggerManager triggerManager;
	
	private String triggerSource;
	
//	private Map<Integer, Trigger> triggersLocalCopy;
	private long lastUpdateTime = -1;
	
	public TriggerBasedScheduleLoader(TriggerManager triggerManager, ExecutorManager executorManager, ProjectManager projectManager, String triggerSource) {
		this.triggerManager = triggerManager;
		this.triggerSource = triggerSource;
		// need to init the action types and condition checker types 
		ExecuteFlowAction.setExecutorManager(executorManager);
		ExecuteFlowAction.setProjectManager(projectManager);
	}
	
	private Trigger scheduleToTrigger(Schedule s) {
		
		Condition triggerCondition = createTimeTriggerCondition(s);
		Condition expireCondition = createTimeExpireCondition(s);
		List<TriggerAction> actions = createActions(s);
		Trigger t = new Trigger(new DateTime(s.getLastModifyTime()), new DateTime(s.getSubmitTime()), s.getSubmitUser(), triggerSource, triggerCondition, expireCondition, actions);
		if(s.isRecurring()) {
			t.setResetOnTrigger(true);
		}
		return t;
	}
	
	private List<TriggerAction> createActions (Schedule s) {
		List<TriggerAction> actions = new ArrayList<TriggerAction>();
		TriggerAction act = new ExecuteFlowAction(s.getProjectId(), s.getProjectName(), s.getFlowName(), s.getSubmitUser(), s.getExecutionOptions());
		actions.add(act);
		return actions;
	}
	
	private Condition createTimeTriggerCondition (Schedule s) {
		Map<String, ConditionChecker> checkers = new HashMap<String, ConditionChecker>();
		ConditionChecker checker = new BasicTimeChecker("BasicTimeChecker_1", new DateTime(s.getFirstSchedTime()), s.getTimezone(), s.isRecurring(), s.skipPastOccurrences(), s.getPeriod());
		checkers.put(checker.getId(), checker);
		String expr = checker.getId() + ".eval()";
		Condition cond = new Condition(checkers, expr);
		return cond;
	}
	
	// if failed to trigger, auto expire?
	private Condition createTimeExpireCondition (Schedule s) {
		Map<String, ConditionChecker> checkers = new HashMap<String, ConditionChecker>();
		ConditionChecker checker = new BasicTimeChecker("BasicTimeChecker_2", new DateTime(s.getFirstSchedTime()), s.getTimezone(), s.isRecurring(), s.skipPastOccurrences(), s.getPeriod());
		checkers.put(checker.getId(), checker);
		String expr = checker.getId() + ".eval()";
		Condition cond = new Condition(checkers, expr);
		return cond;
	}

	@Override
	public void insertSchedule(Schedule s) throws ScheduleManagerException {
		Trigger t = scheduleToTrigger(s);
		try {
			triggerManager.insertTrigger(t);
			s.setScheduleId(t.getTriggerId());
//			triggersLocalCopy.put(t.getTriggerId(), t);
		} catch (TriggerManagerException e) {
			// TODO Auto-generated catch block
			throw new ScheduleManagerException("Failed to insert new schedule!", e);
		}
	}

	@Override
	public void updateSchedule(Schedule s) throws ScheduleManagerException {
		Trigger t = scheduleToTrigger(s);
		try {
			triggerManager.updateTrigger(t);
//			triggersLocalCopy.put(t.getTriggerId(), t);
		} catch (TriggerManagerException e) {
			// TODO Auto-generated catch block
			throw new ScheduleManagerException("Failed to update schedule!", e);
		}
	}

	//TODO
	// may need to add logic to filter out skip runs
	@Override
	public synchronized List<Schedule> loadSchedules() throws ScheduleManagerException {
		List<Trigger> triggers = triggerManager.getTriggers(triggerSource);
		List<Schedule> schedules = new ArrayList<Schedule>();
//		triggersLocalCopy = new HashMap<Integer, Trigger>();
		for(Trigger t : triggers) {
//				triggersLocalCopy.put(t.getTriggerId(), t);
			lastUpdateTime = Math.max(lastUpdateTime, t.getLastModifyTime().getMillis());
			Schedule s = triggerToSchedule(t);
			schedules.add(s);
			System.out.println("loaded schedule for " + s.getProjectId() + s.getProjectName());
		}
		return schedules;
		
	}
	
	private Schedule triggerToSchedule(Trigger t) throws ScheduleManagerException {
		Condition triggerCond = t.getTriggerCondition();
		Map<String, ConditionChecker> checkers = triggerCond.getCheckers();
		BasicTimeChecker ck = null;
		for(ConditionChecker checker : checkers.values()) {
			if(checker.getType().equals(BasicTimeChecker.type)) {
				ck = (BasicTimeChecker) checker;
				break;
			}
		}
		List<TriggerAction> actions = t.getActions();
		ExecuteFlowAction act = null;
		for(TriggerAction action : actions) {
			if(action.getType().equals(ExecuteFlowAction.type)) {
				act = (ExecuteFlowAction) action;
				break;
			}
		}
		if(ck != null && act != null) {
			Schedule s = new Schedule(
					t.getTriggerId(), 
					act.getProjectId(), 
					act.getProjectName(), 
					act.getFlowName(), 
					t.getStatus().toString(), 
					ck.getFirstCheckTime().getMillis(), 
					ck.getFirstCheckTime().getZone(), 
					ck.getPeriod(),
					t.getLastModifyTime().getMillis(),
					ck.getNextCheckTime().getMillis(),
					t.getSubmitTime().getMillis(),
					t.getSubmitUser());
			return s;
		} else {
			logger.error("Failed to parse schedule from trigger!");
			throw new ScheduleManagerException("Failed to parse schedule from trigger!");
		}
	}

	@Override
	public void removeSchedule(Schedule s) throws ScheduleManagerException {
		try {
			triggerManager.removeTrigger(s.getScheduleId());
//			triggersLocalCopy.remove(s.getScheduleId());
		} catch (TriggerManagerException e) {
			// TODO Auto-generated catch block
			throw new ScheduleManagerException(e.getMessage());
		}
		
	}

	@Override
	public void updateNextExecTime(Schedule s)
			throws ScheduleManagerException {
//		Trigger t = triggersLocalCopy.get(s.getScheduleId());
//		BasicTimeChecker ck = (BasicTimeChecker) t.getTriggerCondition().getCheckers().values().toArray()[0];
//		s.setNextExecTime(ck.getNextCheckTime().getMillis());
	}

	@Override
	public synchronized List<Schedule> loadUpdatedSchedules() throws ScheduleManagerException {
		List<Trigger> triggers = triggerManager.getUpdatedTriggers(triggerSource, lastUpdateTime);
		List<Schedule> schedules = new ArrayList<Schedule>();
		for(Trigger t : triggers) {
			lastUpdateTime = Math.max(lastUpdateTime, t.getLastModifyTime().getMillis());
			Schedule s = triggerToSchedule(t);
			schedules.add(s);
			System.out.println("loaded schedule for " + s.getProjectId() + s.getProjectName());
		}
		return schedules;
	}

}
