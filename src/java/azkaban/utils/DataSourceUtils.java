
package azkaban.utils;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;

public class DataSourceUtils {
	public static DataSource getMySQLDataSource(String host, Integer port, String dbName, String user, String password, Integer numConnections) {
		return new MySQLBasicDataSource(host, port, dbName, user, password, numConnections);
	}

	
	private DataSourceUtils() {
	}

	public static class MySQLBasicDataSource extends BasicDataSource {
		private MySQLBasicDataSource(String host, int port, String dbName, String user, String password, int numConnections) {
			super();
			
			String url = "jdbc:mysql://" + (host + ":" + port + "/" + dbName);
			setDriverClassName("com.mysql.jdbc.Driver");
			setUsername(user);
			setPassword(password);
			setUrl(url);
			setMaxActive(numConnections);
			setValidationQuery("/* ping */ select 1");
			setTestOnBorrow(true);
		}
	}
	
	public static void testConnection(DataSource ds) throws SQLException {
		QueryRunner runner = new QueryRunner(ds);
		runner.update("SHOW TABLES");
	}
	
	public static void testMySQLConnection(String host, Integer port, String dbName, String user, String password, Integer numConnections) throws SQLException {
		DataSource ds = new MySQLBasicDataSource(host, port, dbName, user, password, numConnections);
		testConnection(ds);
	}
}
