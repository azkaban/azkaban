package azkaban.test.executor;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;	
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableJobInfo;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionReference;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.executor.Status;
import azkaban.flow.Flow;

import azkaban.database.DataSourceUtils;
import azkaban.project.Project;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;

public class JdbcExecutorLoaderTest {
	private static boolean testDBExists;
	//@TODO remove this and turn into local host.
	private static final String host = "cyu-ld.linkedin.biz";
	private static final int port = 3306;
	private static final String database = "azkaban2";
	private static final String user = "azkaban";
	private static final String password = "azkaban";
	private static final int numConnections = 10;
	
	private File flowDir = new File("unit/executions/exectest1");
	
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
			runner.query(connection, "SELECT COUNT(1) FROM active_executing_flows", countHandler);
		} catch (SQLException e) {
			e.printStackTrace();
			testDBExists = false;
			DbUtils.closeQuietly(connection);
			return;
		}

		try {
			runner.query(connection, "SELECT COUNT(1) FROM execution_flows", countHandler);
		} catch (SQLException e) {
			e.printStackTrace();
			testDBExists = false;
			DbUtils.closeQuietly(connection);
			return;
		}

		try {
			runner.query(connection, "SELECT COUNT(1) FROM execution_jobs", countHandler);
		} catch (SQLException e) {
			e.printStackTrace();
			testDBExists = false;
			DbUtils.closeQuietly(connection);
			return;
		}
		
		try {
			runner.query(connection, "SELECT COUNT(1) FROM execution_logs", countHandler);
		} catch (SQLException e) {
			e.printStackTrace();
			testDBExists = false;
			DbUtils.closeQuietly(connection);
			return;
		}
		
		DbUtils.closeQuietly(connection);
		
		clearDB();
	}
	
	private static void clearDB() {
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
			runner.update(connection, "DELETE FROM active_executing_flows");
			
		} catch (SQLException e) {
			e.printStackTrace();
			testDBExists = false;
			DbUtils.closeQuietly(connection);
			return;
		}

		try {
			runner.update(connection, "DELETE FROM execution_flows");
		} catch (SQLException e) {
			e.printStackTrace();
			testDBExists = false;
			DbUtils.closeQuietly(connection);
			return;
		}
		
		try {
			runner.update(connection, "DELETE FROM execution_jobs");
		} catch (SQLException e) {
			e.printStackTrace();
			testDBExists = false;
			DbUtils.closeQuietly(connection);
			return;
		}

		try {
			runner.update(connection, "DELETE FROM execution_logs");
		} catch (SQLException e) {
			e.printStackTrace();
			testDBExists = false;
			DbUtils.closeQuietly(connection);
			return;
		}
		
		DbUtils.closeQuietly(connection);
	}
	
	@Test
	public void testUploadExecutionFlows() throws Exception {
		if (!isTestSetup()) {
			return;
		}
		
		ExecutorLoader loader = createLoader();
		ExecutableFlow flow = createExecutableFlow("exec1");

		loader.uploadExecutableFlow(flow);
		
		ExecutableFlow fetchFlow = loader.fetchExecutableFlow(flow.getExecutionId());

		// Shouldn't be the same object.
		Assert.assertTrue(flow != fetchFlow);
		Assert.assertEquals(flow.getExecutionId(), fetchFlow.getExecutionId());
		Assert.assertEquals(flow.getEndTime(), fetchFlow.getEndTime());
		Assert.assertEquals(flow.getStartTime(), fetchFlow.getStartTime());
		Assert.assertEquals(flow.getSubmitTime(), fetchFlow.getSubmitTime());
		Assert.assertEquals(flow.getFlowId(), fetchFlow.getFlowId());
		Assert.assertEquals(flow.getProjectId(), fetchFlow.getProjectId());
		Assert.assertEquals(flow.getVersion(), fetchFlow.getVersion());
		Assert.assertEquals(flow.getExecutionOptions().getFailureAction(), fetchFlow.getExecutionOptions().getFailureAction());
		Assert.assertEquals(new HashSet<String>(flow.getEndNodes()), new HashSet<String>(fetchFlow.getEndNodes()));
	}
	
	@Test
	public void testUpdateExecutionFlows() throws Exception {
		if (!isTestSetup()) {
			return;
		}
		
		ExecutorLoader loader = createLoader();
		ExecutableFlow flow = createExecutableFlow("exec1");

		loader.uploadExecutableFlow(flow);
		
		ExecutableFlow fetchFlow2 = loader.fetchExecutableFlow(flow.getExecutionId());
		
		fetchFlow2.setEndTime(System.currentTimeMillis());
		fetchFlow2.setStatus(Status.SUCCEEDED);
		loader.updateExecutableFlow(fetchFlow2);
		ExecutableFlow fetchFlow = loader.fetchExecutableFlow(flow.getExecutionId());
		
		// Shouldn't be the same object.
		Assert.assertTrue(flow != fetchFlow);
		Assert.assertEquals(flow.getExecutionId(), fetchFlow.getExecutionId());
		Assert.assertEquals(fetchFlow2.getEndTime(), fetchFlow.getEndTime());
		Assert.assertEquals(fetchFlow2.getStatus(), fetchFlow.getStatus());
		Assert.assertEquals(flow.getStartTime(), fetchFlow.getStartTime());
		Assert.assertEquals(flow.getSubmitTime(), fetchFlow.getSubmitTime());
		Assert.assertEquals(flow.getFlowId(), fetchFlow.getFlowId());
		Assert.assertEquals(flow.getProjectId(), fetchFlow.getProjectId());
		Assert.assertEquals(flow.getVersion(), fetchFlow.getVersion());
		Assert.assertEquals(flow.getExecutionOptions().getFailureAction(), fetchFlow.getExecutionOptions().getFailureAction());
		Assert.assertEquals(new HashSet<String>(flow.getEndNodes()), new HashSet<String>(fetchFlow.getEndNodes()));
	}
	
	
	@Test
	public void testUploadExecutableNode() throws Exception {
		if (!isTestSetup()) {
			return;
		}
		
		ExecutorLoader loader = createLoader();
		ExecutableFlow flow = createExecutableFlow(10, "exec1");
		flow.setExecutionId(10);
		
		File jobFile = new File(flowDir, "job10.job");
		Props props = new Props(null, jobFile);
		props.put("test","test2");
		ExecutableNode oldNode = flow.getExecutableNode("job10");
		oldNode.setStartTime(System.currentTimeMillis());
		loader.uploadExecutableNode(oldNode, props);
		
		ExecutableJobInfo info = loader.fetchJobInfo(10, "job10", 0);
		Assert.assertEquals(flow.getExecutionId(), info.getExecId());
		Assert.assertEquals(flow.getProjectId(), info.getProjectId());
		Assert.assertEquals(flow.getVersion(), info.getVersion());
		Assert.assertEquals(flow.getFlowId(), info.getFlowId());
		Assert.assertEquals(oldNode.getId(), info.getJobId());
		Assert.assertEquals(oldNode.getStatus(), info.getStatus());
		Assert.assertEquals(oldNode.getStartTime(), info.getStartTime());
		Assert.assertEquals("endTime = " + oldNode.getEndTime() + " info endTime = " + info.getEndTime(), oldNode.getEndTime(), info.getEndTime());
	
		// Fetch props
		Props outputProps = new Props();
		outputProps.put("hello", "output");
		oldNode.setOutputProps(outputProps);
		oldNode.setEndTime(System.currentTimeMillis());
		loader.updateExecutableNode(oldNode);

		Props fInputProps = loader.fetchExecutionJobInputProps(10, "job10");
		Props fOutputProps = loader.fetchExecutionJobOutputProps(10, "job10");
		Pair<Props,Props> inOutProps = loader.fetchExecutionJobProps(10, "job10");
		
		Assert.assertEquals(fInputProps.get("test"), "test2");
		Assert.assertEquals(fOutputProps.get("hello"), "output");
		Assert.assertEquals(inOutProps.getFirst().get("test"), "test2");
		Assert.assertEquals(inOutProps.getSecond().get("hello"), "output");
		
	}
	
	@Test
	public void testActiveReference() throws Exception {
		if (!isTestSetup()) {
			return;
		}
		
		ExecutorLoader loader = createLoader();
		ExecutableFlow flow1 = createExecutableFlow("exec1");
		loader.uploadExecutableFlow(flow1);
		ExecutionReference ref1 = new ExecutionReference(flow1.getExecutionId(), "test", 1);
		loader.addActiveExecutableReference(ref1);
		
		ExecutableFlow flow2 = createExecutableFlow("exec1");
		loader.uploadExecutableFlow(flow2);
		ExecutionReference ref2 = new ExecutionReference(flow2.getExecutionId(), "test", 1);
		loader.addActiveExecutableReference(ref2);
		
		ExecutableFlow flow3 = createExecutableFlow("exec1");
		loader.uploadExecutableFlow(flow3);
		
		Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows1 = loader.fetchActiveFlows();
		ExecutableFlow flow1Result = activeFlows1.get(flow1.getExecutionId()).getSecond();
		Assert.assertNotNull(flow1Result);
		Assert.assertTrue(flow1 != flow1Result);
		Assert.assertEquals(flow1.getExecutionId(), flow1Result.getExecutionId());
		Assert.assertEquals(flow1.getEndTime(), flow1Result.getEndTime());
		Assert.assertEquals(flow1.getStartTime(), flow1Result.getStartTime());
		Assert.assertEquals(flow1.getSubmitTime(), flow1Result.getSubmitTime());
		Assert.assertEquals(flow1.getFlowId(), flow1Result.getFlowId());
		Assert.assertEquals(flow1.getProjectId(), flow1Result.getProjectId());
		Assert.assertEquals(flow1.getVersion(), flow1Result.getVersion());
		Assert.assertEquals(flow1.getExecutionOptions().getFailureAction(), flow1Result.getExecutionOptions().getFailureAction());
		
		ExecutableFlow flow1Result2 = activeFlows1.get(flow2.getExecutionId()).getSecond();
		Assert.assertNotNull(flow1Result2);
		Assert.assertTrue(flow2 != flow1Result2);
		Assert.assertEquals(flow2.getExecutionId(), flow1Result2.getExecutionId());
		Assert.assertEquals(flow2.getEndTime(), flow1Result2.getEndTime());
		Assert.assertEquals(flow2.getStartTime(), flow1Result2.getStartTime());
		Assert.assertEquals(flow2.getSubmitTime(), flow1Result2.getSubmitTime());
		Assert.assertEquals(flow2.getFlowId(), flow1Result2.getFlowId());
		Assert.assertEquals(flow2.getProjectId(), flow1Result2.getProjectId());
		Assert.assertEquals(flow2.getVersion(), flow1Result2.getVersion());
		Assert.assertEquals(flow2.getExecutionOptions().getFailureAction(), flow1Result2.getExecutionOptions().getFailureAction());
		
		loader.removeActiveExecutableReference(flow2.getExecutionId());
		Map<Integer, Pair<ExecutionReference,ExecutableFlow>> activeFlows2 = loader.fetchActiveFlows();

		Assert.assertTrue(activeFlows2.containsKey(flow1.getExecutionId()));
		Assert.assertFalse(activeFlows2.containsKey(flow3.getExecutionId()));
		Assert.assertFalse(activeFlows2.containsKey(flow2.getExecutionId()));
	}
	
	@Test
	public void testSmallUploadLog() throws ExecutorManagerException {
		File logDir = new File("unit/executions/logtest");
		File[] smalllog = {new File(logDir, "log1.log"), new File(logDir, "log2.log"), new File(logDir, "log3.log")};

		ExecutorLoader loader = createLoader();
		loader.uploadLogFile(1, "smallFiles", 0, smalllog);
		
		LogData data = loader.fetchLogs(1, "smallFiles", 0, 0, 50000);
		Assert.assertNotNull(data);
		Assert.assertEquals("Logs length is " + data.getLength(), data.getLength(), 53);
		
		System.out.println(data.toString());
	
		LogData data2 = loader.fetchLogs(1, "smallFiles", 0, 10, 20);
		System.out.println(data2.toString());
		Assert.assertNotNull(data2);
		Assert.assertEquals("Logs length is " + data2.getLength(), data2.getLength(), 20);

	}
	
	@Test
	public void testLargeUploadLog() throws ExecutorManagerException {
		File logDir = new File("unit/executions/logtest");
		
		// Multiple of 255 for Henry the Eigth
		File[] largelog = {new File(logDir, "largeLog1.log"), new File(logDir, "largeLog2.log"), new File(logDir, "largeLog3.log")};

		ExecutorLoader loader = createLoader();
		loader.uploadLogFile(1, "largeFiles",0, largelog);
		
		LogData logsResult = loader.fetchLogs(1, "largeFiles",0, 0, 64000);
		Assert.assertNotNull(logsResult);
		Assert.assertEquals("Logs length is " + logsResult.getLength(), logsResult.getLength(), 64000);
		
		LogData logsResult2 = loader.fetchLogs(1, "largeFiles",0, 1000, 64000);
		Assert.assertNotNull(logsResult2);
		Assert.assertEquals("Logs length is " + logsResult2.getLength(), logsResult2.getLength(), 64000);
		
		LogData logsResult3 = loader.fetchLogs(1, "largeFiles",0, 330000, 400000);
		Assert.assertNotNull(logsResult3);
		Assert.assertEquals("Logs length is " + logsResult3.getLength(), logsResult3.getLength(), 5493);
		
		LogData logsResult4 = loader.fetchLogs(1, "largeFiles",0, 340000, 400000);
		Assert.assertNull(logsResult4);
		
		LogData logsResult5 = loader.fetchLogs(1, "largeFiles",0, 153600, 204800);
		Assert.assertNotNull(logsResult5);
		Assert.assertEquals("Logs length is " + logsResult5.getLength(), logsResult5.getLength(), 181893);
		
		LogData logsResult6 = loader.fetchLogs(1, "largeFiles",0, 150000, 250000);
		Assert.assertNotNull(logsResult6);
		Assert.assertEquals("Logs length is " + logsResult6.getLength(), logsResult6.getLength(), 185493);
	}
	
	@SuppressWarnings("static-access")
	@Test
	public void testRemoveExecutionLogsByTime() throws ExecutorManagerException, IOException, InterruptedException {
		
		ExecutorLoader loader = createLoader();
		
		File logDir = new File("unit/executions/logtest");		
		
		// Multiple of 255 for Henry the Eigth
		File[] largelog = {new File(logDir, "largeLog1.log"), new File(logDir, "largeLog2.log"), new File(logDir, "largeLog3.log")};
		
		DateTime time1 = DateTime.now();
		loader.uploadLogFile(1, "oldlog", 0, largelog);
		// sleep for 5 seconds
		Thread.currentThread().sleep(5000);
		loader.uploadLogFile(2, "newlog", 0, largelog);
		
		DateTime time2 = time1.plusMillis(2500);
				
		int count = loader.removeExecutionLogsByTime(time2.getMillis());
		System.out.print("Removed " + count + " records");
		LogData logs = loader.fetchLogs(1, "oldlog", 0, 0, 22222);
		Assert.assertTrue(logs == null);
		logs = loader.fetchLogs(2, "newlog", 0, 0, 22222);
		Assert.assertFalse(logs == null);
	}
	
	private ExecutableFlow createExecutableFlow(int executionId, String flowName) throws IOException {
		File jsonFlowFile = new File(flowDir, flowName + ".flow");
		@SuppressWarnings("unchecked")
		HashMap<String, Object> flowObj = (HashMap<String, Object>) JSONUtils.parseJSONFromFile(jsonFlowFile);
		
		Flow flow = Flow.flowFromObject(flowObj);
		Project project = new Project(1, "flow");
		HashMap<String, Flow> flowMap = new HashMap<String, Flow>();
		flowMap.put(flow.getId(), flow);
		project.setFlows(flowMap);
		ExecutableFlow execFlow = new ExecutableFlow(project, flow);
		execFlow.setExecutionId(executionId);

		return execFlow;
	}
	
	private ExecutableFlow createExecutableFlow(String flowName) throws IOException {
		File jsonFlowFile = new File(flowDir, flowName + ".flow");
		@SuppressWarnings("unchecked")
		HashMap<String, Object> flowObj = (HashMap<String, Object>) JSONUtils.parseJSONFromFile(jsonFlowFile);
		
		Flow flow = Flow.flowFromObject(flowObj);
		Project project = new Project(1, "flow");
		HashMap<String, Flow> flowMap = new HashMap<String, Flow>();
		flowMap.put(flow.getId(), flow);
		project.setFlows(flowMap);
		ExecutableFlow execFlow = new ExecutableFlow(project, flow);

		return execFlow;
	}
	
	private ExecutorLoader createLoader() {
		Props props = new Props();
		props.put("database.type", "mysql");
		
		props.put("mysql.host", host);		
		props.put("mysql.port", port);
		props.put("mysql.user", user);
		props.put("mysql.database", database);
		props.put("mysql.password", password);
		props.put("mysql.numconnections", numConnections);
		
		return new JdbcExecutorLoader(props);
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