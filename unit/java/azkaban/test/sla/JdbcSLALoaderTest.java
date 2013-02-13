package azkaban.test.sla;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import azkaban.sla.JdbcSLALoader;
import azkaban.sla.SLA;
import azkaban.sla.SLA.SlaAction;
import azkaban.sla.SLA.SlaRule;
import azkaban.sla.SLALoader;
import azkaban.utils.DataSourceUtils;
import azkaban.utils.Props;



public class JdbcSLALoaderTest {
	private static boolean testDBExists;
	//@TODO remove this and turn into local host.
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
			runner.query(connection, "SELECT COUNT(1) FROM active_sla", countHandler);
		} catch (SQLException e) {
			e.printStackTrace();
			testDBExists = false;
			DbUtils.closeQuietly(connection);
			return;
		}

		DbUtils.closeQuietly(connection);
		
		clearDB();
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

		QueryRunner runner = new QueryRunner();
		try {
			runner.update(connection, "DELETE FROM active_sla");
			
		} catch (SQLException e) {
			e.printStackTrace();
			testDBExists = false;
			DbUtils.closeQuietly(connection);
			return;
		}
		
		DbUtils.closeQuietly(connection);
	}
	
	@Test
	public void testInsertSLA() throws Exception {
		if (!isTestSetup()) {
			return;
		}
		
		SLALoader loader = createLoader();
		
		int execId = 1;
		String jobName = "";
		DateTime checkTime = new DateTime(11111);
		List<String> emails = new ArrayList<String>();
		emails.add("email1");
		emails.add("email2");
//		List<SlaRule> rules = new ArrayList<SlaRule>();
//		rules.add(SlaRule.SUCCESS);
//		rules.add(SlaRule.FINISH);
		List<SlaAction> actions = new ArrayList<SLA.SlaAction>();
		actions.add(SlaAction.EMAIL);
		SLA s = new SLA(execId, jobName, checkTime, emails, actions, null, SlaRule.FINISH);

		loader.insertSLA(s);
		
		List<SLA> allSLAs = loader.loadSLAs();
		SLA fetchSLA = allSLAs.get(0);

		Assert.assertTrue(allSLAs.size() == 1);
		// Shouldn't be the same object.
		Assert.assertTrue(s != fetchSLA);
		Assert.assertEquals(s.getExecId(), fetchSLA.getExecId());
		Assert.assertEquals(s.getJobName(), fetchSLA.getJobName());
		Assert.assertEquals(s.getCheckTime(), fetchSLA.getCheckTime());
		Assert.assertEquals(s.getEmails(), fetchSLA.getEmails());
		Assert.assertEquals(s.getRule(), fetchSLA.getRule());
		Assert.assertEquals(s.getActions().get(0), fetchSLA.getActions().get(0));
		
		
		loader.removeSLA(s);
		
		allSLAs = loader.loadSLAs();
		
		Assert.assertTrue(allSLAs.size() == 0);
	}
	
	private SLALoader createLoader() {
		Props props = new Props();
		props.put("database.type", "mysql");
		
		props.put("mysql.host", host);		
		props.put("mysql.port", port);
		props.put("mysql.user", user);
		props.put("mysql.database", database);
		props.put("mysql.password", password);
		props.put("mysql.numconnections", numConnections);
		
		return new JdbcSLALoader(props);
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