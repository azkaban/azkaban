package azkaban.scheduler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import junit.framework.Assert;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import azkaban.executor.ExecutionOptions;
import azkaban.sla.SLA.SlaAction;
import azkaban.sla.SLA.SlaRule;
import azkaban.sla.SLA.SlaSetting;
import azkaban.sla.SlaOptions;
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
		
		List<String> emails = new ArrayList<String>();
		emails.add("email1");
		emails.add("email2");
		List<String> disabledJobs = new ArrayList<String>();
		disabledJobs.add("job1");
		disabledJobs.add("job2");
		List<SlaSetting> slaSets = new ArrayList<SlaSetting>();
		SlaSetting set1 = new SlaSetting();
		List<SlaAction> actions = new ArrayList<SlaAction>();
		actions.add(SlaAction.EMAIL);
		set1.setActions(actions);
		set1.setId("");
		set1.setDuration(Schedule.parsePeriodString("1h"));
		set1.setRule(SlaRule.FINISH);
		slaSets.add(set1);
		ExecutionOptions flowOptions = new ExecutionOptions();
		flowOptions.setFailureEmails(emails);
		flowOptions.setDisabledJobs(disabledJobs);
		SlaOptions slaOptions = new SlaOptions();
		slaOptions.setSlaEmails(emails);
		slaOptions.setSettings(slaSets);
		
		Schedule s1 = new Schedule(1, "proj1", "flow1", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu", flowOptions, slaOptions);
		Schedule s2 = new Schedule(1, "proj1", "flow2", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "ccc", flowOptions, slaOptions);
		Schedule s3 = new Schedule(2, "proj1", "flow1", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu", flowOptions, slaOptions);
		Schedule s4 = new Schedule(3, "proj2", "flow1", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu", flowOptions, slaOptions);
		Schedule s5 = new Schedule(3, "proj2", "flow2", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu", flowOptions, slaOptions);
		Schedule s6 = new Schedule(3, "proj2", "flow3", "error", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu", flowOptions, slaOptions);
		
		loader.insertSchedule(s1);
		loader.insertSchedule(s2);
		loader.insertSchedule(s3);
		loader.insertSchedule(s4);
		loader.insertSchedule(s5);
		loader.insertSchedule(s6);
		
		List<Schedule> schedules = loader.loadSchedules();
		Schedule sched = schedules.get(0);
		
		Assert.assertEquals(6, schedules.size());
		Assert.assertEquals("America/Los_Angeles", sched.getTimezone().getID());
		Assert.assertEquals(44444, sched.getSubmitTime());
		Assert.assertEquals("1d", Schedule.createPeriodString(sched.getPeriod()));
		ExecutionOptions fOpt = sched.getExecutionOptions();
		SlaOptions sOpt = sched.getSlaOptions();
		Assert.assertEquals(SlaAction.EMAIL, sOpt.getSettings().get(0).getActions().get(0));
		Assert.assertEquals("", sOpt.getSettings().get(0).getId());
		Assert.assertEquals(Schedule.parsePeriodString("1h"), sOpt.getSettings().get(0).getDuration());
		Assert.assertEquals(SlaRule.FINISH, sOpt.getSettings().get(0).getRule());
		Assert.assertEquals(2, fOpt.getFailureEmails().size());
		Assert.assertEquals(null, fOpt.getSuccessEmails());
		Assert.assertEquals(2, fOpt.getDisabledJobs().size());
		Assert.assertEquals(ExecutionOptions.FailureAction.FINISH_CURRENTLY_RUNNING, fOpt.getFailureAction());
		Assert.assertEquals(null, fOpt.getFlowParameters());
	}
	
	@Test
	public void testInsertAndUpdateSchedule() throws ScheduleManagerException {
		if (!isTestSetup()) {
			return;
		}
		clearDB();
		
		JdbcScheduleLoader loader = createLoader();
		
		List<String> emails = new ArrayList<String>();
		emails.add("email1");
		emails.add("email2");
		List<String> disabledJobs = new ArrayList<String>();
		disabledJobs.add("job1");
		disabledJobs.add("job2");
		List<SlaSetting> slaSets = new ArrayList<SlaSetting>();
		SlaSetting set1 = new SlaSetting();
		List<SlaAction> actions = new ArrayList<SlaAction>();
		actions.add(SlaAction.EMAIL);
		set1.setActions(actions);
		set1.setId("");
		set1.setDuration(Schedule.parsePeriodString("1h"));
		set1.setRule(SlaRule.FINISH);
		slaSets.add(set1);
		ExecutionOptions flowOptions = new ExecutionOptions();
		flowOptions.setFailureEmails(emails);
		flowOptions.setDisabledJobs(disabledJobs);
		SlaOptions slaOptions = new SlaOptions();
		slaOptions.setSlaEmails(emails);
		slaOptions.setSettings(slaSets);
		
		System.out.println("the flow options are " + flowOptions);
		System.out.println("the sla options are " + slaOptions);
		Schedule s1 = new Schedule(1, "proj1", "flow1", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu", flowOptions, slaOptions);

		loader.insertSchedule(s1);
		
		emails.add("email3");
		slaOptions.setSlaEmails(emails);
		
		Schedule s2 = new Schedule(1, "proj1", "flow1", "ready", 11112, "America/Los_Angeles", "2M", 22223, 33334, 44445, "cyu", flowOptions, slaOptions);

		loader.updateSchedule(s2);
		
		List<Schedule> schedules = loader.loadSchedules();
		
		Assert.assertEquals(1, schedules.size());
		Assert.assertEquals("America/Los_Angeles", schedules.get(0).getTimezone().getID());
		Assert.assertEquals(44445, schedules.get(0).getSubmitTime());
		Assert.assertEquals("2M", Schedule.createPeriodString(schedules.get(0).getPeriod()));
//		System.out.println("the options are " + schedules.get(0).getSchedOptions());
		Assert.assertEquals(3, schedules.get(0).getSlaOptions().getSlaEmails().size());
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
			List<String> emails = new ArrayList<String>();
			emails.add("email1");
			emails.add("email2");
			List<String> disabledJobs = new ArrayList<String>();
			disabledJobs.add("job1");
			disabledJobs.add("job2");
			List<SlaSetting> slaSets = new ArrayList<SlaSetting>();
			SlaSetting set1 = new SlaSetting();
			List<SlaAction> actions = new ArrayList<SlaAction>();
			actions.add(SlaAction.EMAIL);
			set1.setActions(actions);
			set1.setId("");
			set1.setDuration(Schedule.parsePeriodString("1h"));
			set1.setRule(SlaRule.FINISH);
			slaSets.add(set1);
			ExecutionOptions flowOptions = new ExecutionOptions();
			flowOptions.setFailureEmails(emails);
			flowOptions.setDisabledJobs(disabledJobs);
			SlaOptions slaOptions = new SlaOptions();
			slaOptions.setSlaEmails(emails);
			slaOptions.setSettings(slaSets);
			
			Schedule s = new Schedule(i+1, "proj"+(i+1), "flow1", "ready", 11111, "America/Los_Angeles", "1d", 22222, 33333, 44444, "cyu", flowOptions, slaOptions);
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
