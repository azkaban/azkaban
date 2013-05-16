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

import azkaban.database.AzkabanDatabaseUpdater;
import azkaban.database.DataSourceUtils;
import azkaban.utils.Props;

public class AzkabanDatabaseUpdaterTest {
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
	public void testMySQLAutoCreate() throws Exception {
		String confDir = "unit/conf/dbtestmysql";
		System.out.println("1.***Now testing check");
		AzkabanDatabaseUpdater.main(new String[]{"-c",confDir});
		
		System.out.println("2.***Now testing update");
		AzkabanDatabaseUpdater.main(new String[]{ "-u", "-c",confDir});
		
		System.out.println("3.***Now testing check again");
		AzkabanDatabaseUpdater.main(new String[]{ "-c",confDir});
		
		System.out.println("4.***Now testing update again");
		AzkabanDatabaseUpdater.main(new String[]{ "-c",confDir, "-u"});
		
		System.out.println("5.***Now testing check again");
		AzkabanDatabaseUpdater.main(new String[]{ "-c",confDir});
	}
	
	@Test
	public void testH2AutoCreate() throws Exception {
		String confDir = "unit/conf/dbtesth2";
		System.out.println("1.***Now testing check");
		AzkabanDatabaseUpdater.main(new String[]{"-c",confDir});
		
		System.out.println("2.***Now testing update");
		AzkabanDatabaseUpdater.main(new String[]{ "-u", "-c",confDir});
		
		System.out.println("3.***Now testing check again");
		AzkabanDatabaseUpdater.main(new String[]{ "-c",confDir});
		
		System.out.println("4.***Now testing update again");
		AzkabanDatabaseUpdater.main(new String[]{ "-c",confDir, "-u"});
		
		System.out.println("5.***Now testing check again");
		AzkabanDatabaseUpdater.main(new String[]{ "-c",confDir});
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
