package azkaban.jmx;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;

public class JmxJettyServer implements JmxJettyServerMBean {
	private Server server;
	private Connector connector;
	
	public JmxJettyServer(Server server) {
		this.server = server;
		this.connector = server.getConnectors()[0];
	}
	
	@Override
	public boolean isRunning() {
		return this.server.isRunning();
	}
	
	@Override
	public boolean isFailed() {
		return this.server.isFailed();
	}
	
	@Override
	public boolean isStopped() {
		return this.server.isStopped();
	}
	
	@Override
	public int getNumThreads() {
		return this.server.getThreadPool().getThreads();
	}
	
	@Override
	public int getNumIdleThreads() {
		return this.server.getThreadPool().getIdleThreads();
	}
	
	@Override
	public String getHost() {
		return connector.getHost();
	}
	
	@Override
	public int getPort() {
		return connector.getPort();
	}

	@Override
	public int getConfidentialPort() {
		return connector.getConfidentialPort();
	}
	
	@Override
	public int getConnections() {
		return connector.getConnections();
	}
	
	@Override
	public int getConnectionsOpen() {
		return connector.getConnectionsOpen();
	}
	
	@Override
	public int getConnectionsOpenMax() {
		return connector.getConnectionsOpenMax();
	}
	
	@Override
	public int getConnectionsOpenMin() {
		return connector.getConnectionsOpenMin();
	}
	
	@Override
	public long getConnectionsDurationAve() {
		return connector.getConnectionsDurationAve();
	}
	
	@Override
	public long getConnectionsDurationMax() {
		return connector.getConnectionsDurationMax();
	}
	
	@Override
	public long getConnectionsDurationMin() {
		return connector.getConnectionsDurationMin();
	}
	
	@Override
	public long getConnectionsDurationTotal() {
		return connector.getConnectionsDurationTotal();
	}
	
	@Override
	public long getConnectionsRequestAve() {
		return connector.getConnectionsRequestsAve();
	}
	
	@Override
	public long getConnectionsRequestMax() {
		return connector.getConnectionsRequestsMax();
	}
	
	@Override
	public long getConnectionsRequestMin() {
		return connector.getConnectionsRequestsMin();
	}
	
	@Override
	public void turnStatsOn() {
		connector.setStatsOn(true);
	}
	
	@Override
	public void turnStatsOff() {
		connector.setStatsOn(false);
	}
	
	@Override
	public void resetStats() {
		connector.statsReset();
	}

	@Override
	public boolean isStatsOn() {
		return connector.getStatsOn();
	}
}
