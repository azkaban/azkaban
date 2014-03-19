package azkaban.jmx;

public interface JmxJettyServerMBean {
	@DisplayName("OPERATION: isRunning")
	public boolean isRunning();
	
	@DisplayName("OPERATION: isFailed")
	public boolean isFailed();
	
	@DisplayName("OPERATION: isStopped")
	public boolean isStopped();
	
	@DisplayName("OPERATION: getNumThreads")
	public int getNumThreads();
	
	@DisplayName("OPERATION: getNumIdleThreads")
	public int getNumIdleThreads();
	
	@DisplayName("OPERATION: getHost")
	public String getHost();
	
	@DisplayName("OPERATION: getPort")
	public int getPort();

	@DisplayName("OPERATION: getConfidentialPort")
	public int getConfidentialPort();
	
	@DisplayName("OPERATION: getConnections")
	public int getConnections();
	
	@DisplayName("OPERATION: getConnectionsOpen")
	public int getConnectionsOpen();
	
	@DisplayName("OPERATION: getConnectionsOpenMax")
	public int getConnectionsOpenMax();
	
	@DisplayName("OPERATION: getConnectionsOpenMin")
	public int getConnectionsOpenMin();
	
	@DisplayName("OPERATION: getConnectionsDurationAve")
	public long getConnectionsDurationAve();
	
	@DisplayName("OPERATION: getConnectionsDurationMax")
	public long getConnectionsDurationMax();
	
	@DisplayName("OPERATION: getConnectionsDurationMin")
	public long getConnectionsDurationMin();
	
	@DisplayName("OPERATION: getConnectionsDurationTotal")
	public long getConnectionsDurationTotal();
	
	@DisplayName("OPERATION: getConnectionsRequestAve")
	public long getConnectionsRequestAve();
	
	@DisplayName("OPERATION: getConnectionsRequestMax")
	public long getConnectionsRequestMax();
	
	@DisplayName("OPERATION: getConnectionsRequestMin")
	public long getConnectionsRequestMin();
	
	@DisplayName("OPERATION: turnStatsOn")
	public void turnStatsOn();
	
	@DisplayName("OPERATION: turnStatsOff")
	public void turnStatsOff();
	
	@DisplayName("OPERATION: resetStats")
	public void resetStats();
		
	@DisplayName("OPERATION: isStatsOn")
	public boolean isStatsOn();
}
