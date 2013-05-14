package azkaban.test.utils.db;

import java.io.File;
import java.io.IOException;
import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import azkaban.utils.db.DataSourceUtils;
import azkaban.utils.db.H2TableSetup;

public class H2TableSetupTest {
	private static DataSource datasource;
	
	@BeforeClass
	public static void setupDB() throws IOException {
		File dbDir = new File("h2dbtest");
		if (dbDir.exists()) {
			FileUtils.deleteDirectory(dbDir);
		}
		
		dbDir.mkdir();
		datasource = DataSourceUtils.getH2DataSource("h2dbtest/h2db");
	}
	
	@AfterClass
	public static void teardownDB() {
	}
	
	@Test
	public void queryTables() throws Exception {
		H2TableSetup setup = new H2TableSetup(datasource);
		setup.createProjectTables();
		setup.createExecutionTables();
		setup.createOtherTables();
		setup.setupTables();
	}
}
