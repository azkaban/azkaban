package azkaban.scheduler;

import java.util.List;


public interface ScheduleLoader {
	public void saveSchedule(List<ScheduledFlow> schedule);
	
	public List<ScheduledFlow> loadSchedule();

}