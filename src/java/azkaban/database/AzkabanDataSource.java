package azkaban.database;

import org.apache.commons.dbcp.BasicDataSource;

public abstract class AzkabanDataSource extends BasicDataSource {
	public abstract boolean allowsOnDuplicateKey();
	
	public abstract String getDBType();
}