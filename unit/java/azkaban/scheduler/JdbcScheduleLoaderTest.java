package azkaban.scheduler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import junit.framework.Assert;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.joda.time.DateTimeZone;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


import azkaban.utils.DataSourceUtils;
import azkaban.utils.Props;

public class JdbcScheduleLoaderTest {
	private static boolean testDBExists;
	private static final String host = "localhost";
	private static final int port = 3306;
	private static final String database = "azkaban2";
	private static final String user = "azkaban";
	private static final String password = "azkaban";
	private static final int numConnections = 10;
	
	@BeforeClass
	public static void setupDB() {
		DataSource dataSource = DataSourceUtils.getMySQLDataSource(host, port, database, user, password, numConnections);
		testDBExists = true;
		
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
			testDBExists = false;
			DbUtils.closeQuietly(connection);
			return;
		}

		CountHandler countHandler = new CountHandler();
		QueryRunner runner = new QueryRunner();
		try {
			runner.query(connection, "SELECT COUNT(1) FROM schedules", countHandler);
		} catch (SQLException e) {
			e.printStackTrace();
			testDBExists = false;
			DbUtils.closeQuietly(connection);
			return;
		}
		finally {
			DbUtils.closeQuietly(connection);
		}
	}
	
	@AfterClass
	public static void clearDB() {
		if (!testDBExists) {
			return;
		}

		DataSource dataSource = DataSourceUtils.getMySQLDataSource(host, port, database, user, password, numConnections);
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
			testDBExists = false;
			DbUtils.closeQuietly(connection);
			return;
		}

//		CountHandler countHandler = new CountHandler();
		QueryRunner runner = new QueryRunner();
		try {
			runner.update(connection, "DELETE FROM schedules");
		} catch (SQLException e) {
			e.printStackTrace();
			testDBExists = false;
			DbUtils.closeQuietly(connection);
			return;
		}
		finally {
			DbUtils.closeQuietly(connection);
		}
	}
	
	@Test
	public void testInsertAndLoadSchedule() throws ScheduleManagerException {
		if (!isTestSetup()) {
			return;
		}
		clearDB();
		
		JdbcScheduleLoader loader = createLoader();
		
		Map<String, Object> scheduleOptions = new HashMap<String, Object>();
		List<String> disabled = new ArrayList<String>();
		disabled.add("job1");
		disabled.add("job2");
		disabled.add("job3");
		List<String> failEmails = new ArrayList<String>();
		failEmails.add("email1");
		failEmails.add("email2");
		failEmails.add("email3");
		boolean hasSla = true;
		scheduleOptions.put("disabled", disabled);
		scheduleOptions.put("failEmails", failEmails);
		scheduleOptions.put("hasSla", hasSla);
		
		Schedule s1 = new Schedule(1, "proj1", "flow1", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu", scheduleOptions);
		Schedule s2 = new Schedule(1, "proj1", "flow2", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "ccc", scheduleOptions);
		Schedule s3 = new Schedule(2, "proj1", "flow1", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu", scheduleOptions);
		Schedule s4 = new Schedule(3, "proj2", "flow1", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu", scheduleOptions);
		Schedule s5 = new Schedule(3, "proj2", "flow2", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu", scheduleOptions);
		Schedule s6 = new Schedule(3, "proj2", "flow3", "error", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu", scheduleOptions);
		
		loader.insertSchedule(s1);
		loader.insertSchedule(s2);
		loader.insertSchedule(s3);
		loader.insertSchedule(s4);
		loader.insertSchedule(s5);
		loader.insertSchedule(s6);
		
		List<Schedule> schedules = loader.loadSchedules();
		
		Assert.assertEquals(6, schedules.size());
		Assert.assertEquals("America/Los_Angeles", schedules.get(0).getTimezone().getID());
		Assert.assertEquals(44444, schedules.get(0).getSubmitTime());
		Assert.assertEquals("1d", Schedule.createPeriodString(schedules.get(0).getPeriod()));
		System.out.println("the options are " + schedules.get(0).getSchedOptions());
		Assert.assertEquals(true, schedules.get(0).getSchedOptions().get("hasSla"));
	}
	
	@Test
	public void testInsertAndUpdateSchedule() throws ScheduleManagerException {
		if (!isTestSetup()) {
			return;
		}
		clearDB();
		
		JdbcScheduleLoader loader = createLoader();
		
		Map<String, Object> scheduleOptions = new HashMap<String, Object>();
		List<String> disabled = new ArrayList<String>();
		disabled.add("job1");
		disabled.add("job2");
		disabled.add("job3");
		List<String> failEmails = new ArrayList<String>();
		failEmails.add("email1");
		failEmails.add("email2");
		failEmails.add("email3");
		boolean hasSla = true;
		scheduleOptions.put("disabled", disabled);
		scheduleOptions.put("failEmails", failEmails);
		scheduleOptions.put("hasSla", hasSla);
		
		System.out.println("the options are " + scheduleOptions);
		Schedule s1 = new Schedule(1, "proj1", "flow1", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu", scheduleOptions);

		loader.insertSchedule(s1);
		
		hasSla = false;
		scheduleOptions.put("hasSla", hasSla);
		
		Schedule s2 = new Schedule(1, "proj1", "flow1", "ready", 11112, "America/Los_Angeles", "2M", 22223, 33334, 44445, "cyu", scheduleOptions);

		loader.updateSchedule(s2);
		
		List<Schedule> schedules = loader.loadSchedules();
		
		Assert.assertEquals(1, schedules.size());
		Assert.assertEquals("America/Los_Angeles", schedules.get(0).getTimezone().getID());
		Assert.assertEquals(44445, schedules.get(0).getSubmitTime());
		Assert.assertEquals("2M", Schedule.createPeriodString(schedules.get(0).getPeriod()));
		System.out.println("the options are " + schedules.get(0).getSchedOptions());
		Assert.assertEquals(false, schedules.get(0).getSchedOptions().get("hasSla"));
	}
	
	@Test
	public void testInsertAndRemoveSchedule() {
		if (!testDBExists) {
			return;
		}
		
		clearDB();
		
		JdbcScheduleLoader loader = createLoader();
		
		List<Schedule> schedules = new ArrayList<Schedule>();
		
		int stress = 10;
		
		for(int i=0; i<stress; i++)
		{
			Map<String, Object> scheduleOptions = new HashMap<String, Object>();
			List<String> disabled = new ArrayList<String>();
			disabled.add("job1");
			disabled.add("job2");
			disabled.add("job3");
			List<String> failEmails = new ArrayList<String>();
			failEmails.add("email1");
			failEmails.add("email2");
			failEmails.add("email3");
			boolean hasSla = true;
			scheduleOptions.put("disabled", disabled);
			scheduleOptions.put("failEmails", failEmails);
			scheduleOptions.put("hasSla", hasSla);
			
			Schedule s = new Schedule(i+1, "proj"+(i+1), "flow1", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu", scheduleOptions);
			schedules.add(s);
			try {
				loader.insertSchedule(s);
			} catch (ScheduleManagerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		for(Schedule s : schedules)
		{
			try {
				loader.removeSchedule(s);
			} catch (ScheduleManagerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// should have cleaned up
		List<Schedule> otherSchedules = null;
		try {
			otherSchedules = loader.loadSchedules();
		} catch (ScheduleManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Assert.assertEquals(0, otherSchedules.size());
		
	}
	
	
	private JdbcScheduleLoader createLoader() {
		Props props = new Props();
		props.put("database.type", "mysql");
		
		props.put("mysql.host", host);		
		props.put("mysql.port", port);
		props.put("mysql.user", user);
		props.put("mysql.database", database);
		props.put("mysql.password", password);
		props.put("mysql.numconnections", numConnections);
		
		return new JdbcScheduleLoader(props);
	}
	
	private boolean isTestSetup() {
		if (!testDBExists) {
			System.err.println("Skipping DB test because Db not setup.");
			return false;
		}
		
		System.out.println("Running DB test because Db setup.");
		return true;
	}
	
	public static class CountHandler implements ResultSetHandler<Integer> {
		@Override
		public Integer handle(ResultSet rs) throws SQLException {
			int val = 0;
			while (rs.next()) {
				val++;
			}
			
			return val;
		}
		
	}
}
