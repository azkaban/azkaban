package azkaban.utils.db;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class H2TableSetup {
	private static final Logger logger = Logger.getLogger(H2TableSetup.class);
	
	private DataSource dataSource;
	private Map<String, TableData> tables = new HashMap<String,TableData>();
	
	public H2TableSetup(DataSource source) throws SQLException {
		this.dataSource = source;
		setupTables();
	}
	
	public void setupTables() throws SQLException {
		Connection conn = dataSource.getConnection();
		ResultSet rs = conn.getMetaData().getTables(conn.getCatalog(), null, null, new String[]{"TABLE"});
		
		while(rs.next()) {
			TableData data = new TableData(rs.getString("TABLE_NAME").toLowerCase());
			data.setSchema(rs.getString("TABLE_SCHEMA"));
			tables.put(data.getName(), data);
			System.out.println(data.toString());
		}
		
		conn.close();
	}
	
	public void createExecutionTables() throws SQLException, IOException {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();

			if (!tables.containsKey("execution_flows")) {
				logger.info("Creating execution_flows table.");
				URL url = this.getClass().getClassLoader().getResource("azkaban/utils/db/h2/execution_flows.sql");
				String query = IOUtils.toString(url.openStream());
				QueryRunner runner = new QueryRunner();
				runner.update(conn, query);
				conn.commit();
			}
			if (!tables.containsKey("execution_jobs")) {
				logger.info("Creating execution_jobs table.");
				URL url = this.getClass().getClassLoader().getResource("azkaban/utils/db/h2/execution_jobs.sql");
				String query = IOUtils.toString(url.openStream());
				QueryRunner runner = new QueryRunner();
				runner.update(conn, query);
				conn.commit();
			}
			if (!tables.containsKey("execution_logs")) {
				logger.info("Creating execution_logs table.");
				URL url = this.getClass().getClassLoader().getResource("azkaban/utils/db/h2/execution_logs.sql");
				String query = IOUtils.toString(url.openStream());
				QueryRunner runner = new QueryRunner();
				runner.update(conn, query);
				conn.commit();
			}
			if (!tables.containsKey("active_executing_flows")) {
				logger.info("Creating active_executing_flows table.");
				URL url = this.getClass().getClassLoader().getResource("azkaban/utils/db/h2/active_executing_flows.sql");
				String query = IOUtils.toString(url.openStream());
				QueryRunner runner = new QueryRunner();
				runner.update(conn, query);
				conn.commit();
			}
		}
		finally {
			conn.close();
		}
	}
	
	public void createOtherTables() throws SQLException, IOException {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();

			if (!tables.containsKey("active_sla")) {
				logger.info("Creating active_sla table.");
				URL url = this.getClass().getClassLoader().getResource("azkaban/utils/db/h2/active_sla.sql");
				String query = IOUtils.toString(url.openStream());
				QueryRunner runner = new QueryRunner();
				runner.update(conn, query);
				conn.commit();
			}
			if (!tables.containsKey("schedules")) {
				logger.info("Creating schedules table.");
				URL url = this.getClass().getClassLoader().getResource("azkaban/utils/db/h2/schedules.sql");
				String query = IOUtils.toString(url.openStream());
				QueryRunner runner = new QueryRunner();
				runner.update(conn, query);
				conn.commit();
			}
		}
		finally {
			conn.close();
		}
	}
	
	public void createProjectTables() throws SQLException, IOException {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();

			if (!tables.containsKey("projects")) {
				logger.info("Creating projects table.");
				URL url = this.getClass().getClassLoader().getResource("azkaban/utils/db/h2/projects.sql");
				String query = IOUtils.toString(url.openStream());
				QueryRunner runner = new QueryRunner();
				runner.update(conn, query);
				conn.commit();
			}
			if (!tables.containsKey("project_versions")) {
				logger.info("Creating project_versions table.");
				URL url = this.getClass().getClassLoader().getResource("azkaban/utils/db/h2/project_versions.sql");
				String query = IOUtils.toString(url.openStream());
				QueryRunner runner = new QueryRunner();
				runner.update(conn, query);
				conn.commit();
			}
			if (!tables.containsKey("project_events")) {
				logger.info("Creating project_events table.");
				URL url = this.getClass().getClassLoader().getResource("azkaban/utils/db/h2/project_events.sql");
				String query = IOUtils.toString(url.openStream());
				QueryRunner runner = new QueryRunner();
				runner.update(conn, query);
				conn.commit();
			}
			if (!tables.containsKey("project_properties")) {
				logger.info("Creating project_properties table.");
				URL url = this.getClass().getClassLoader().getResource("azkaban/utils/db/h2/project_properties.sql");
				String query = IOUtils.toString(url.openStream());
				QueryRunner runner = new QueryRunner();
				runner.update(conn, query);
				conn.commit();
			}
			if (!tables.containsKey("project_files")) {
				logger.info("Creating project_files table.");
				URL url = this.getClass().getClassLoader().getResource("azkaban/utils/db/h2/project_files.sql");
				String query = IOUtils.toString(url.openStream());
				QueryRunner runner = new QueryRunner();
				runner.update(conn, query);
				conn.commit();
			}
			if (!tables.containsKey("project_permissions")) {
				logger.info("Creating project_permissions table.");
				URL url = this.getClass().getClassLoader().getResource("azkaban/utils/db/h2/project_permissions.sql");
				String query = IOUtils.toString(url.openStream());
				QueryRunner runner = new QueryRunner();
				runner.update(conn, query);
				conn.commit();
			}
			if (!tables.containsKey("project_flows")) {
				logger.info("Creating project_flows table.");
				URL url = this.getClass().getClassLoader().getResource("azkaban/utils/db/h2/project_flows.sql");
				String query = IOUtils.toString(url.openStream());
				QueryRunner runner = new QueryRunner();
				runner.update(conn, query);
				conn.commit();
			}
		}
		finally {
			conn.close();
		}
	}
}
