package azkaban.scheduler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.DurationFieldType;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.ReadablePeriod;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.utils.JSONUtils;

/**
 * Loads the schedule from a schedule file that is JSON like. The format would be as follows:
 * 
 * {
 * 		schedule: [
 * 			{
 * 				"project": "<project>",
 * 				"user": "<user>",
 * 				"flow": "<flow>",
 * 				"time": "<time>",
 * 				"recurrence":"<period>",
 * 				"dependency":<boolean>
 * 			}
 * 		]
 * }	
 * 
 * 
 * @author rpark
 *
 */
public class LocalFileScheduleLoader implements ScheduleLoader {
	private static final String SCHEDULEID = "scheduleId";
	private static final String USER = "user";
	private static final String USERSUBMIT = "userSubmit";
    private static final String SUBMITTIME = "submitTime";
    private static final String FIRSTSCHEDTIME = "firstSchedTime";
	
	private static final String SCHEDULE = "schedule";
	private static final String NEXTEXECTIME = "nextExecTime";
	private static final String TIMEZONE = "timezone";
	private static final String RECURRENCE = "recurrence";
	
	private static final String SCHEDULESTATUS = "schedulestatus";
	
	private static DateTimeFormatter FILE_DATEFORMAT = DateTimeFormat.forPattern("yyyy-MM-dd.HH.mm.ss.SSS");
    private static Logger logger = Logger.getLogger(LocalFileScheduleLoader.class);

    private File basePath;
	private File scheduleFile;
	private File backupScheduleFile;
	
	public LocalFileScheduleLoader(Props props) throws IOException {
			
			
		basePath = new File(props.getString("schedule.directory"));
		if (!basePath.exists()) {
			logger.info("Schedule directory " + basePath + " not found.");
			if (basePath.mkdirs()) {
				logger.info("Schedule directory " + basePath + " created.");
			}
			else {
				throw new RuntimeException("Schedule directory " + basePath + " does not exist and cannot be created.");
			}
		}
			
		scheduleFile = new File(basePath, "schedule");
		if(!scheduleFile.exists() || scheduleFile.isDirectory()) {
			logger.info("Schedule file " + scheduleFile + " not found.");
			if(scheduleFile.createNewFile() && scheduleFile.canRead() && scheduleFile.canWrite()) {
				logger.info("Schedule file " + scheduleFile + " created.");
			}
			else {
				throw new RuntimeException("Schedule file " + scheduleFile + " cannot be created.");
			}
		}

		backupScheduleFile = new File(basePath, "backup");
		if(!backupScheduleFile.exists() || backupScheduleFile.isDirectory()) {
			logger.info("Backup schedule file " + backupScheduleFile + " not found.");
			if(backupScheduleFile.createNewFile() && backupScheduleFile.canRead() && backupScheduleFile.canWrite()) {
				logger.info("Backup schedule file " + backupScheduleFile + " created.");
			}
			else {
				throw new RuntimeException("Backup schedule file " + backupScheduleFile + " cannot be created.");
			}
		}

	}


	@Override
	public List<ScheduledFlow> loadSchedule() {
        if (scheduleFile != null && backupScheduleFile != null) {
            if (scheduleFile.exists()) {
            	if(scheduleFile.length() == 0)
            		return new ArrayList<ScheduledFlow>();
				return loadFromFile(scheduleFile);
            }
            else if (backupScheduleFile.exists()) {
            	backupScheduleFile.renameTo(scheduleFile);
				return loadFromFile(scheduleFile);
            }
            else {
                logger.warn("No schedule files found looking for " + scheduleFile.getAbsolutePath());
            }
        }
        
        return new ArrayList<ScheduledFlow>();
	}

	@Override
	public void saveSchedule(List<ScheduledFlow> schedule) {
        if (scheduleFile != null && backupScheduleFile != null) {
            // Delete the backup if it exists and a current file exists.
            if (backupScheduleFile.exists() && scheduleFile.exists()) {
            	backupScheduleFile.delete();
            }

            // Rename the schedule if it exists.
            if (scheduleFile.exists()) {
            	scheduleFile.renameTo(backupScheduleFile);
            }

            HashMap<String,Object> obj = new HashMap<String,Object>();
            ArrayList<Object> schedules = new ArrayList<Object>();
            obj.put(SCHEDULE, schedules);
            //Write out schedule.
                       
            for (ScheduledFlow schedFlow : schedule) {
            	schedules.add(createJSONObject(schedFlow));
            }
 
    		try {
    			FileWriter writer = new FileWriter(scheduleFile);
    			writer.write(JSONUtils.toJSON(obj, true));
    			writer.flush();
    		} catch (Exception e) {
    			throw new RuntimeException("Error saving flow file", e);
    		}
    		logger.info("schedule saved");
        }
	}
	
    @SuppressWarnings("unchecked")
	private List<ScheduledFlow> loadFromFile(File schedulefile)
    {
    	BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(schedulefile));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			logger.error("Error loading schedule file ", e);
		}
    	List<ScheduledFlow> scheduleList = new ArrayList<ScheduledFlow>();
    	
		HashMap<String, Object> schedule;
		try {
			//TODO handle first time empty schedule file
			schedule = (HashMap<String,Object>)JSONUtils.parseJSONFromReader(reader);
		} catch (Exception e) {
			//schedule = loadLegacyFile(schedulefile);
			logger.error("Error parsing the schedule file", e);
			throw new RuntimeException("Error parsing the schedule file", e);
		}
		finally {
			try {
				reader.close();
			} catch (IOException e) {
			}
		}
		
		ArrayList<Object> array = (ArrayList<Object>)schedule.get("schedule");
		for (int i = 0; i < array.size(); ++i) {
			HashMap<String, Object> schedItem = (HashMap<String, Object>)array.get(i);
			ScheduledFlow sched = createScheduledFlow(schedItem);
			if (sched != null) {
				scheduleList.add(sched);	
			}
		}
		
		return scheduleList;
    }

    private ScheduledFlow createScheduledFlow(HashMap<String, Object> obj) {
    	String scheduleId = (String)obj.get(SCHEDULEID);
    	String user = (String)obj.get(USER);
    	String userSubmit = (String)obj.get(USERSUBMIT);
    	String submitTimeRaw = (String)obj.get(SUBMITTIME);
    	String firstSchedTimeRaw = (String)obj.get(FIRSTSCHEDTIME);
    	String nextExecTimeRaw = (String)obj.get(NEXTEXECTIME);
    	String timezone = (String)obj.get(TIMEZONE);
    	String recurrence = (String)obj.get(RECURRENCE);
//    	String scheduleStatus = (String)obj.get(SCHEDULESTATUS);
    	
    	DateTime nextExecTime = FILE_DATEFORMAT.parseDateTime(nextExecTimeRaw);
    	DateTime submitTime = FILE_DATEFORMAT.parseDateTime(submitTimeRaw);
    	DateTime firstSchedTime = FILE_DATEFORMAT.parseDateTime(firstSchedTimeRaw);

    	if (nextExecTime == null) {
    		logger.error("No next execution time has been set");
    		return null;
    	}
    	
    	if (submitTime == null) {
    		logger.error("No submitTime has been set");
    	}
    	
    	if(firstSchedTime == null){
    		logger.error("No first scheduled time has been set");
    	}
    	
    	if (timezone != null) {
    		nextExecTime = nextExecTime.withZoneRetainFields(DateTimeZone.forID(timezone));
    	}

        ReadablePeriod period = null;
    	if (recurrence != null) {
    		period = parsePeriodString(scheduleId, recurrence);
    	}

    	ScheduledFlow scheduledFlow = new ScheduledFlow(scheduleId, user, userSubmit, submitTime, firstSchedTime, nextExecTime, period);
    	if (scheduledFlow.updateTime()) {
    		return scheduledFlow;
    	}
    	
    	logger.info("Removed " + scheduleId + " off out of scheduled. It is not recurring.");
    	return null;
    }
    
	private HashMap<String,Object> createJSONObject(ScheduledFlow flow) {
    	HashMap<String,Object> object = new HashMap<String,Object>();
    	object.put(SCHEDULEID, flow.getScheduleId());
    	object.put(USER, flow.getUser());
    	object.put(USERSUBMIT, flow.getUserSubmit());
   
    	object.put(SUBMITTIME, FILE_DATEFORMAT.print(flow.getSubmitTime()));
    	object.put(FIRSTSCHEDTIME, FILE_DATEFORMAT.print(flow.getFirstSchedTime()));
    	
    	object.put(NEXTEXECTIME, FILE_DATEFORMAT.print(flow.getNextExecTime()));
    	object.put(TIMEZONE, flow.getNextExecTime().getZone().getID());
    	object.put(RECURRENCE, createPeriodString(flow.getPeriod()));
//    	object.put(SCHEDULESTATUS, flow.getSchedStatus());
    	
    	return object;
    }
    
    private ReadablePeriod parsePeriodString(String scheduleId, String periodStr)
    {
        ReadablePeriod period;
        char periodUnit = periodStr.charAt(periodStr.length() - 1);
        if (periodUnit == 'n') {
            return null;
        }

        int periodInt = Integer.parseInt(periodStr.substring(0, periodStr.length() - 1));
        switch (periodUnit) {
            case 'd':
                period = Days.days(periodInt);
                break;
            case 'h':
                period = Hours.hours(periodInt);
                break;
            case 'm':
                period = Minutes.minutes(periodInt);
                break;
            case 's':
                period = Seconds.seconds(periodInt);
                break;
            default:
                throw new IllegalArgumentException("Invalid schedule period unit '" + periodUnit + "' for flow " + scheduleId);
        }

        return period;
    }

    private String createPeriodString(ReadablePeriod period)
    {
        String periodStr = "n";

        if (period == null) {
            return "n";
        }

        if (period.get(DurationFieldType.days()) > 0) {
            int days = period.get(DurationFieldType.days());
            periodStr = days + "d";
        }
        else if (period.get(DurationFieldType.hours()) > 0) {
            int hours = period.get(DurationFieldType.hours());
            periodStr = hours + "h";
        }
        else if (period.get(DurationFieldType.minutes()) > 0) {
            int minutes = period.get(DurationFieldType.minutes());
            periodStr = minutes + "m";
        }
        else if (period.get(DurationFieldType.seconds()) > 0) {
            int seconds = period.get(DurationFieldType.seconds());
            periodStr = seconds + "s";
        }

        return periodStr;
    }

//    private HashMap<String,Object> loadLegacyFile(File schedulefile) {
//        Props schedule = null;
//        try {
//            schedule = new Props(null, schedulefile.getAbsolutePath());
//        } catch(Exception e) {
//            throw new RuntimeException("Error loading schedule from " + schedulefile);
//        }
//
//        ArrayList<Object> flowScheduleList = new ArrayList<Object>();
//        for(String key: schedule.getKeySet()) {
//        	HashMap<String,Object> scheduledMap = parseScheduledFlow(key, schedule.get(key));
//        	if (scheduledMap == null) {
//        		flowScheduleList.add(scheduledMap);
//        	}
//        }
//
//        HashMap<String,Object> scheduleMap = new HashMap<String,Object>();
//        scheduleMap.put(SCHEDULE, flowScheduleList );
//        
//        return scheduleMap;
//    }
    
//    private HashMap<String,Object> parseScheduledFlow(String name, String flow) {
//        String[] pieces = flow.split("\\s+");
//
//        if(pieces.length != 3) {
//            logger.warn("Error loading schedule from file " + name);
//            return null;
//        }
//
//        HashMap<String,Object> scheduledFlow = new HashMap<String,Object>();
//        scheduledFlow.put(PROJECTID, name);
//        scheduledFlow.put(TIME, pieces[0]);
//        scheduledFlow.put(RECURRENCE, pieces[1]);
//        Boolean dependency = Boolean.parseBoolean(pieces[2]);
//        
//        return scheduledFlow;
//    }
}