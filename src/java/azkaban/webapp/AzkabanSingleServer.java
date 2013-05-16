package azkaban.webapp;


import org.apache.log4j.Logger;

import azkaban.database.AzkabanDatabaseSetup;
import azkaban.database.AzkabanDatabaseUpdater;
import azkaban.execapp.AzkabanExecutorServer;
import azkaban.utils.Props;

public class AzkabanSingleServer {
	private static final Logger logger = Logger.getLogger(AzkabanWebServer.class);
	public static void main(String[] args) throws Exception {
		logger.info("Starting Azkaban Server");
		
		Props props = AzkabanServer.loadProps(args);
		if (props == null) {
			logger.error("Properties not found. Need it to connect to the db.");
			logger.error("Exiting...");
			return;
		}

		boolean checkversion = props.getBoolean(AzkabanDatabaseSetup.DATABASE_CHECK_VERSION, true);

		if (checkversion) {
			boolean updateDB = props.getBoolean(AzkabanDatabaseSetup.DATABASE_AUTO_UPDATE_TABLES, true);
			String scriptDir = props.getString(AzkabanDatabaseSetup.DATABASE_SQL_SCRIPT_DIR, "sql");
			AzkabanDatabaseUpdater.runDatabaseUpdater(props, scriptDir, updateDB);
		}
		
		AzkabanWebServer.main(args);
		logger.info("Azkaban Web Server started...");
		AzkabanExecutorServer.main(args);
		logger.info("Azkaban Exec Server started...");
	}
}