package azkaban.test.database;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import azkaban.database.AzkabanDatabaseSetup;
import azkaban.database.DataSourceUtils;
import azkaban.utils.Props;

public class AzkabanDatabaseSetupTest {
	@BeforeClass
	public static void setupDB() throws IOException, SQLException {
		File dbDir = new File("h2dbtest");
		if (dbDir.exists()) {
			FileUtils.deleteDirectory(dbDir);
		}
		
		dbDir.mkdir();
		
		clearUnitTestDB();
	}
	
	@AfterClass
	public static void teardownDB() {
	}
	
	@Test
	public void testH2Query() throws Exception {
		Props h2Props = getH2Props();
		AzkabanDatabaseSetup setup = new AzkabanDatabaseSetup(h2Props);
		setup.checkTableVersion(true, true);
		
		setup.checkTableVersion(true, true);
	}
	
	@Test
	public void testMySQLQuery() throws Exception {
		Props mysqlProps = getMySQLProps();
		AzkabanDatabaseSetup setup = new AzkabanDatabaseSetup(mysqlProps);
		setup.checkTableVersion(true, true);
		
		setup.checkTableVersion(true, true);
	}
	
	private static Props getH2Props() {
		Props props = new Props();
		props.put("database.type", "h2");
		props.put("h2.path", "h2dbtest/h2db");
		props.put("sql.script.path", "unit/sql");
		
		return props;
	}
	
	private static Props getMySQLProps() {
		Props props = new Props();

		props.put("database.type", "mysql");
		props.put("mysql.port", "3306");
		props.put("mysql.host", "localhost");
		props.put("mysql.database", "azkabanunittest");
		props.put("mysql.user", "root");
		props.put("sql.script.path", "unit/sql");
		props.put("mysql.password", "");
		props.put("mysql.numconnections", 10);
		
		return props;
	}
	
	private static void clearUnitTestDB() throws SQLException {
		Props props = new Props();

		props.put("database.type", "mysql");
		props.put("mysql.host", "localhost");
		props.put("mysql.port", "3306");
		props.put("mysql.database", "");
		props.put("mysql.user", "root");
		props.put("mysql.password", "");
		props.put("mysql.numconnections", 10);
		
		DataSource datasource = DataSourceUtils.getDataSource(props);
		QueryRunner runner = new QueryRunner(datasource);
		try {
			runner.update("drop database azkabanunittest");
		} catch (SQLException e) {
		}
		runner.update("create database azkabanunittest");
	}
}