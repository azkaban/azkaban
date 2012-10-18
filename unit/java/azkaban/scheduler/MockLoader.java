package azkaban.scheduler;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Period;

public class MockLoader implements ScheduleLoader {
	private ArrayList<ScheduledFlow> scheduledFlow = new ArrayList<ScheduledFlow>();
	
	public void addScheduledFlow(String scheduleId, String projectId, String flowId, String user, String userSubmit, DateTime submitTime, DateTime firstSchedTime, DateTime nextExec, Period recurrence) {
		ScheduledFlow flow = new ScheduledFlow(scheduleId, projectId, flowId, user, userSubmit, submitTime, firstSchedTime, nextExec, recurrence);
		addScheduleFlow(flow);
	}
	
	public void addScheduleFlow(ScheduledFlow flow) {
		scheduledFlow.add(flow);
	}
	
	public void clearSchedule() {
		scheduledFlow.clear();
	}
	
	@Override
	public List<ScheduledFlow> loadSchedule() {
		return scheduledFlow;
	}

	@Override
	public void saveSchedule(List<ScheduledFlow> schedule) {
		scheduledFlow.clear();
		scheduledFlow.addAll(schedule);
	}
	
}