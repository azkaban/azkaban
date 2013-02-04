/*
 * Copyright 2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.scheduler;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;

import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import azkaban.utils.DataSourceUtils;
import azkaban.utils.Props;


public class JdbcScheduleLoader implements ScheduleLoader {
	
	private DataSource dataSource;
	private static DateTimeFormatter FILE_DATEFORMAT = DateTimeFormat.forPattern("yyyy-MM-dd.HH.mm.ss.SSS");
    private static Logger logger = Logger.getLogger(JdbcScheduleLoader.class);
	
	private static final String SCHEDULE = "schedule";
	// schedule ids
//	private static final String PROJECTGUID = "projectGuid";
//	private static final String FLOWGUID = "flowGuid";
//	
//	private static final String SCHEDULEID = "scheduleId";
	private static final String PROJECTID = "projectId";
	private static final String PROJECTNAME = "projectName";
	private static final String FLOWNAME = "flowName";
	// status
	private static final String STATUS = "status";
	// schedule info
	private static final String FIRSTSCHEDTIME = "firstSchedTime";
	private static final String TIMEZONE = "timezone";
	private static final String PERIOD = "period";
	private static final String LASTMODIFYTIME = "lastModifyTime";
	private static final String NEXTEXECTIME = "nextExecTime";
	// auditing info
	private static final String SUBMITTIME = "submitTime";
	private static final String SUBMITUSER = "userSubmit";
	
	private static final String scheduleTableName = "schedules";

	private static String SELECT_SCHEDULE_BY_KEY = 
			"SELECT project_id, project_name, flow_name, status, first_sched_time, timezone, period, last_modify_time, next_exec_time, submit_time, submit_user FROM " + scheduleTableName + " WHERE project_id=? AND flow_name=?";
	
	private static String SELECT_ALL_SCHEDULES =
			"SELECT project_id, project_name, flow_name, status, first_sched_time, timezone, period, last_modify_time, next_exec_time, submit_time, submit_user FROM " + scheduleTableName;
	
	private static String INSERT_SCHEDULE = 
			"INSERT INTO " + scheduleTableName + " ( project_id, project_name, flow_name, status, first_sched_time, timezone, period, last_modify_time, next_exec_time, submit_time, submit_user) values (?,?,?,?,?,?,?,?,?,?,?)";
	
	private static String REMOVE_SCHEDULE_BY_KEY = 
			"DELETE FROM " + scheduleTableName + " WHERE project_id=? AND flow_name=?";
	
	private static String UPDATE_SCHEDULE_BY_KEY = 
			"UPDATE " + scheduleTableName + " SET status=?, first_sched_time=?, timezone=?, period=?, last_modify_time=?, next_exec_time=?, submit_time=?, submit_user=? WHERE project_id=? AND flow_name=?";
	
	private static String UPDATE_NEXT_EXEC_TIME = 
			"UPDATE " + scheduleTableName + " SET next_exec_time=? WHERE project_id=? AND flow_name=?";


	public JdbcScheduleLoader(Props props) {
		String databaseType = props.getString("database.type");
		
		if (databaseType.equals("mysql")) {
			int port = props.getInt("mysql.port");
			String host = props.getString("mysql.host");
			String database = props.getString("mysql.database");
			String user = props.getString("mysql.user");
			String password = props.getString("mysql.password");
			int numConnections = props.getInt("mysql.numconnections");
			
			dataSource = DataSourceUtils.getMySQLDataSource(host, port, database, user, password, numConnections);
		}
	}

	@Override
	public List<Schedule> loadSchedules() throws ScheduleManagerException {
		logger.info("Loading all schedules from db.");
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e1) {
			logger.error("Failed to get db connection!");
			e1.printStackTrace();
			DbUtils.closeQuietly(connection);
			throw new ScheduleManagerException("Failed to get db connection!", e1);
		}

		QueryRunner runner = new QueryRunner();
		ResultSetHandler<List<Schedule>> handler = new ScheduleResultHandler();
	
		List<Schedule> schedules;
		
		try {
			schedules = runner.query(connection, SELECT_ALL_SCHEDULES, handler);
		} catch (SQLException e) {
			logger.error(SELECT_ALL_SCHEDULES + " failed.");

			DbUtils.closeQuietly(connection);
			throw new ScheduleManagerException("Loading schedules from db failed. ", e);
		}
		
		// filter the schedules
		for(Schedule sched : schedules) {
			if(!sched.updateTime()) {
				logger.info("Schedule " + sched.getScheduleName() + " was scheduled before azkaban start, skipping it.");
				schedules.remove(sched);
				removeSchedule(sched);
			}
			else {
				try {
					updateNextExecTime(sched, connection);
				} catch (Exception e) {
					DbUtils.closeQuietly(connection);
					throw new ScheduleManagerException("Update next execution time failed.", e);
				}
				logger.info("Schedule " + sched.getScheduleName() + " loaded and updated.");
			}
		}
		
		DbUtils.closeQuietly(connection);
				
		logger.info("Loaded " + schedules.size() + " schedules.");
		
		return schedules;
	}

	@Override
	public void removeSchedule(Schedule s) throws ScheduleManagerException {		
		logger.info("Removing schedule " + s.getScheduleName() + " from db.");

		QueryRunner runner = new QueryRunner(dataSource);
	
		try {
			int removes =  runner.update(REMOVE_SCHEDULE_BY_KEY, s.getProjectId(), s.getFlowName());
			if (removes == 0) {
				throw new ScheduleManagerException("No schedule has been removed.");
			}
		} catch (SQLException e) {
			logger.error(REMOVE_SCHEDULE_BY_KEY + " failed.");
			throw new ScheduleManagerException("Remove schedule " + s.getScheduleName() + " from db failed. ", e);
		}
	}
	
	@Override
	public void insertSchedule(Schedule s) throws ScheduleManagerException {
		logger.info("Inserting schedule " + s.getScheduleName() + " into db.");

		QueryRunner runner = new QueryRunner(dataSource);
		try {
			int inserts =  runner.update( 
					INSERT_SCHEDULE, 
					s.getProjectId(),
					s.getProjectName(),
					s.getFlowName(), 
					s.getStatus(), 
					s.getFirstSchedTime(), 
					s.getTimezone().getID(), 
					Schedule.createPeriodString(s.getPeriod()), 
					s.getLastModifyTime(), 
					s.getNextExecTime(), 
					s.getSubmitTime(), 
					s.getSubmitUser());
			if (inserts == 0) {
				throw new ScheduleManagerException("No schedule has been inserted.");
			}
		} catch (SQLException e) {
			logger.error(INSERT_SCHEDULE + " failed.");
			throw new ScheduleManagerException("Insert schedule " + s.getScheduleName() + " into db failed. ", e);
		}
	}
	
	private void updateNextExecTime(Schedule s, Connection connection) throws ScheduleManagerException 
	{
		QueryRunner runner = new QueryRunner();
		try {
			runner.update(connection, UPDATE_NEXT_EXEC_TIME, s.getNextExecTime(), s.getProjectId(), s.getFlowName()); 
		} catch (SQLException e) {
			logger.error(UPDATE_NEXT_EXEC_TIME + " failed.");
			throw new ScheduleManagerException("Update schedule " + s.getScheduleName() + " into db failed. ", e);
		}
	}
	
	@Override
	public void updateSchedule(Schedule s) throws ScheduleManagerException {
		logger.info("Updating schedule " + s.getScheduleName() + " into db.");

		QueryRunner runner = new QueryRunner(dataSource);
	
		try {
			int updates =  runner.update( 
					UPDATE_SCHEDULE_BY_KEY, 
					s.getStatus(), 
					s.getFirstSchedTime(), 
					s.getTimezone().getID(), 
					Schedule.createPeriodString(s.getPeriod()), 
					s.getLastModifyTime(), 
					s.getNextExecTime(), 
					s.getSubmitTime(), 
					s.getSubmitUser(), 
					s.getProjectId(), 
					s.getFlowName());
			if (updates == 0) {
				throw new ScheduleManagerException("No schedule has been updated.");
			}
		} catch (SQLException e) {
			logger.error(UPDATE_SCHEDULE_BY_KEY + " failed.");
			throw new ScheduleManagerException("Update schedule " + s.getScheduleName() + " into db failed. ", e);
		}
	}
	
//	private Schedule fromJSON(HashMap<String, Object> obj) {
//		long projectGuid = Long.valueOf((String) obj.get(PROJECTGUID));
//		String projectId = (String) obj.get(PROJECTID);
//		long flowGuid = Long.valueOf((String) obj.get(FLOWGUID));
//		String flowId = (String) obj.get(FLOWID);
//		String status = (String) obj.get(STATUS);
//		long firstSchedTime = Long.valueOf((String) obj.get(FIRSTSCHEDTIME));
//		String timezone = (String) obj.get(TIMEZONE);
//		String period = (String) obj.get(PERIOD);
//		long lastModifyTime = Long.valueOf((String) obj.get(LASTMODIFYTIME));
//		long nextExecTime = Long.valueOf((String) obj.get(NEXTEXECTIME));
//		long submitTime = Long.valueOf((String) obj.get(SUBMITTIME));
//		String submitUser = (String) obj.get(SUBMITUSER);
//		
//		return new Schedule(projectId, flowId, status, firstSchedTime, timezone, period, lastModifyTime, nextExecTime, submitTime, submitUser);
//
//	}
//
//	private HashMap<String, Object> toJSON(Schedule s) {
//		HashMap<String, Object> object = new HashMap<String, Object>();
////		object.put(PROJECTGUID, s.getProjectGuid());
//		object.put(SCHEDULEID, s.getScheduleId());
//		object.put(PROJECTID, s.getProjectId());
////		object.put(FLOWGUID, s.getFlowGuid());
//		object.put(FLOWID, s.getFlowId());
//		
//		object.put(STATUS, s.getStatus());
//		
//		object.put(FIRSTSCHEDTIME, s.getFirstSchedTime());
//		object.put(TIMEZONE, s.getTimezone());
//		object.put(PERIOD, s.getPeriod());
//		
//		object.put(LASTMODIFYTIME, s.getLastModifyTime());
//		object.put(NEXTEXECTIME, s.getNextExecTime());
//		object.put(SUBMITTIME, s.getSubmitTime());
//		object.put(SUBMITUSER, s.getSubmitUser());
//
//		return object;
//	}

	public class ScheduleResultHandler implements ResultSetHandler<List<Schedule>> {
		@Override
		public List<Schedule> handle(ResultSet rs) throws SQLException {
			if (!rs.next()) {
				return Collections.<Schedule>emptyList();
			}
			
			ArrayList<Schedule> schedules = new ArrayList<Schedule>();
			do {
				int projectId = rs.getInt(1);
				String projectName = rs.getString(2);
				String flowName = rs.getString(3);
				String status = rs.getString(4);
				long firstSchedTime = rs.getLong(5);
				DateTimeZone timezone = DateTimeZone.forID(rs.getString(6));
				ReadablePeriod period = Schedule.parsePeriodString(rs.getString(7));
				long lastModifyTime = rs.getLong(8);
				long nextExecTime = rs.getLong(9);
				long submitTime = rs.getLong(10);
				String submitUser = rs.getString(11);
				
				Schedule s = new Schedule(projectId, projectName, flowName, status, firstSchedTime, timezone, period, lastModifyTime, nextExecTime, submitTime, submitUser);
				
				schedules.add(s);
			} while (rs.next());
			
			return schedules;
		}
		
	}
}