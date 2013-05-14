package azkaban.utils.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;

import azkaban.utils.Props;
import azkaban.utils.db.H2TableSetup;

public abstract class AbstractJdbcLoader {
	private boolean allowsOnDuplicateKey = false;
	
	/**
	 * Used for when we store text data. Plain uses UTF8 encoding.
	 */
	public static enum EncodingType {
		PLAIN(1), GZIP(2);

		private int numVal;

		EncodingType(int numVal) {
			this.numVal = numVal;
		}

		public int getNumVal() {
			return numVal;
		}

		public static EncodingType fromInteger(int x) {
			switch (x) {
			case 1:
				return PLAIN;
			case 2:
				return GZIP;
			default:
				return PLAIN;
			}
		}
	}
	
	private DataSource dataSource;
	
	public static void setupTables(Props props) throws SQLException, IOException {
		String databaseType = props.getString("database.type");
		
		if (databaseType.equals("h2")) {
			String path = props.getString("h2.path");
			DataSource dataSource = DataSourceUtils.getH2DataSource(path);
			H2TableSetup tableSetup = new H2TableSetup(dataSource);
			
			tableSetup.createProjectTables();
			tableSetup.createExecutionTables();
			tableSetup.createOtherTables();
		}
	}
	
	public AbstractJdbcLoader(Props props) {
		String databaseType = props.getString("database.type");
		
		if (databaseType.equals("mysql")) {
			int port = props.getInt("mysql.port");
			String host = props.getString("mysql.host");
			String database = props.getString("mysql.database");
			String user = props.getString("mysql.user");
			String password = props.getString("mysql.password");
			int numConnections = props.getInt("mysql.numconnections");
			
			allowsOnDuplicateKey = true;
			dataSource = DataSourceUtils.getMySQLDataSource(host, port, database, user, password, numConnections);
		}
		else if (databaseType.equals("h2")) {
			String path = props.getString("h2.path");
			dataSource = DataSourceUtils.getH2DataSource(path);
		}
	}
	
	protected Connection getDBConnection(boolean autoCommit) throws IOException {
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
			connection.setAutoCommit(autoCommit);
		} catch (Exception e) {
			DbUtils.closeQuietly(connection);
			throw new IOException("Error getting DB connection.", e);
		}
		
		return connection;
	}

	protected QueryRunner createQueryRunner()  {
		return new QueryRunner(dataSource);
	}
	
	protected boolean allowsOnDuplicateKey() {
		return allowsOnDuplicateKey;
	}
}