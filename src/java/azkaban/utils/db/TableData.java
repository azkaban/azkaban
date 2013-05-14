package azkaban.utils.db;

public class TableData {
	private String name;
	private String schema;
	
	public TableData(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
	
	public void setSchema(String schema) {
		this.schema = schema;
	}
	
	public String getSchema() {
		return this.schema;
	}

	public String toString() {
		return name + ": " + schema;
	}
}
