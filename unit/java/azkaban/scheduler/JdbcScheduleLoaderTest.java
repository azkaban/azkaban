package azkaban.scheduler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import javax.sql.DataSource;

import junit.framework.Assert;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


import azkaban.utils.DataSourceUtils;
import azkaban.utils.Props;

public class JdbcScheduleLoaderTest {
	private static boolean testDBExists;
	private static final String host = "localhost";
	private static final int port = 3306;
	private static final String database = "azkaban";
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
			int count = runner.query(connection, "SELECT COUNT(1) FROM schedules", countHandler);
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
			int count = runner.update(connection, "DELETE FROM schedules");
			
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
	
//	@Test
//	public void testLoadSchedule() {
//		if (!testDBExists) {
//			return;
//		}
//
//		DataSource dataSource = DataSourceUtils.getMySQLDataSource(host, port, database, user, password, numConnections);
//		Connection connection = null;
//		try {
//			connection = dataSource.getConnection();
//		} catch (SQLException e) {
//			e.printStackTrace();
//			testDBExists = false;
//			DbUtils.closeQuietly(connection);
//			return;
//		}
//
////		CountHandler countHandler = new CountHandler();
//		QueryRunner runner = new QueryRunner();
//		try {
//			int count = runner.update(connection, "DELETE FROM schedules");
//			
//		} catch (SQLException e) {
//			e.printStackTrace();
//			testDBExists = false;
//			DbUtils.closeQuietly(connection);
//			return;
//		}
//		finally {
//			DbUtils.closeQuietly(connection);
//		}
//	}
	
	@Test
	public void testInsertAndLoadSchedule() throws ScheduleManagerException {
		if (!isTestSetup()) {
			return;
		}
		clearDB();
		
		JdbcScheduleLoader loader = createLoader();
		
		Schedule s1 = new Schedule(1, "proj1", "flow1", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu");
		Schedule s2 = new Schedule(1, "proj1", "flow2", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "ccc");
		Schedule s3 = new Schedule(2, "proj1", "flow1", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu");
		Schedule s4 = new Schedule(3, "proj2", "flow1", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu");
		Schedule s5 = new Schedule(3, "proj2", "flow2", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu");
		Schedule s6 = new Schedule(3, "proj2", "flow3", "error", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu");
		
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
		
	}
	
	@Test
	public void testInsertAndRemoveSchedule() {
		if (!testDBExists) {
			return;
		}
		
		clearDB();
		
		JdbcScheduleLoader loader = createLoader();
		
		List<Schedule> schedules = new ArrayList<Schedule>();
		
		int stress = 100;
		
		for(int i=0; i<stress; i++)
		{
			Schedule s = new Schedule(i+1, "proj"+(i+1), "flow1", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu");
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
