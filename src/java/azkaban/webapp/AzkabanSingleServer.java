package azkaban.webapp;


import org.apache.log4j.Logger;

import azkaban.execapp.AzkabanExecutorServer;

public class AzkabanSingleServer {
	private static final Logger logger = Logger.getLogger(AzkabanWebServer.class);
	public static void main(String[] args) throws Exception {
		logger.info("Starting Azkaban Server");
		
		AzkabanWebServer.main(args);
		logger.info("Azkaban Web Server started...");
		AzkabanExecutorServer.main(args);
		logger.info("Azkaban Exec Server started...");
	}
}