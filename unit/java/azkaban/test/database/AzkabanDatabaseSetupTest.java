package azkaban.test.database;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
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
		
		// First time will create the tables
		setup.loadTableInfo();
		setup.printUpgradePlan();
		setup.updateDatabase(true, true);
		Assert.assertTrue(setup.needsUpdating());
		
		// Second time will update some tables. This is only for testing purpose and obviously we
		// wouldn't set things up this way.
		setup.loadTableInfo();
		setup.printUpgradePlan();
		setup.updateDatabase(true, true);
		Assert.assertTrue(setup.needsUpdating());
		
		// Nothing to be done
		setup.loadTableInfo();
		setup.printUpgradePlan();
		Assert.assertFalse(setup.needsUpdating());
	}
	
	@Test
	public void testMySQLQuery() throws Exception {
		Props mysqlProps = getMySQLProps();
		AzkabanDatabaseSetup setup = new AzkabanDatabaseSetup(mysqlProps);
		
		// First time will create the tables
		setup.loadTableInfo();
		setup.printUpgradePlan();
		setup.updateDatabase(true, true);
		Assert.assertTrue(setup.needsUpdating());
		
		// Second time will update some tables. This is only for testing purpose and obviously we
		// wouldn't set things up this way.
		setup.loadTableInfo();
		setup.printUpgradePlan();
		setup.updateDatabase(true, true);
		Assert.assertTrue(setup.needsUpdating());
		
		// Nothing to be done
		setup.loadTableInfo();
		setup.printUpgradePlan();
		Assert.assertFalse(setup.needsUpdating());
	}
	
	private static Props getH2Props() {
		Props props = new Props();
		props.put("database.type", "h2");
		props.put("h2.path", "h2dbtest/h2db");
		props.put("database.sql.scripts.dir", "unit/sql");
		
		return props;
	}
	
	private static Props getMySQLProps() {
		Props props = new Props();

		props.put("database.type", "mysql");
		props.put("mysql.port", "3306");
		props.put("mysql.host", "localhost");
		props.put("mysql.database", "azkabanunittest");
		props.put("mysql.user", "root");
		props.put("database.sql.scripts.dir", "unit/sql");
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