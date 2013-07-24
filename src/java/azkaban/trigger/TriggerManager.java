/*
 * Copyright 2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.trigger;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.Thread.State;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import azkaban.triggerapp.TriggerConnectorParams;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;

/**
 * Executor manager used to manage the client side job.
 *
 */
public class TriggerManager {
	private static Logger logger = Logger.getLogger(TriggerManager.class);

	private static final String TRIGGER_SUFFIX = ".trigger";
	
	private TriggerLoader triggerLoader;
	private CheckerTypeLoader checkerTypeLoader;
	private ActionTypeLoader actionTypeLoader;
	
	private String triggerServerHost;
	private int triggerServerPort;
	
	private Set<Pair<String, Integer>> triggerServers = new HashSet<Pair<String,Integer>>();
	
	private ConcurrentHashMap<Integer, Trigger> triggerIdMap = new ConcurrentHashMap<Integer, Trigger>();
	
	private Map<String, TriggerAgent> triggerAgents = new HashMap<String, TriggerAgent>();

	private TriggerManagerUpdaterThread triggerManagingThread;
	
	private long lastThreadCheckTime = -1;
	
	private long lastUpdateTime = -1;
	
	public TriggerManager(Props props, TriggerLoader loader) throws TriggerManagerException {
		this.triggerLoader = loader;
		this.checkerTypeLoader = new CheckerTypeLoader();
		this.actionTypeLoader = new ActionTypeLoader();

		triggerServerHost = props.getString("trigger.server.host", "localhost");
		triggerServerPort = props.getInt("trigger.server.port");

		triggerManagingThread = new TriggerManagerUpdaterThread();
		
		try{
			checkerTypeLoader.init(props);
			actionTypeLoader.init(props);
		} catch(Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		
		Condition.setCheckerLoader(checkerTypeLoader);
		Trigger.setActionTypeLoader(actionTypeLoader);
		
		triggerServers.add(new Pair<String, Integer>(triggerServerHost, triggerServerPort));

	}
	
	public void start() throws Exception {
		loadTriggers();
		for(TriggerAgent agent : triggerAgents.values()) {
			agent.start();
		}
		triggerManagingThread.start();
	}
	
	private static class SuffixFilter implements FileFilter {
		private String suffix;
		public SuffixFilter(String suffix) {
			this.suffix = suffix;
		}

		@Override
		public boolean accept(File pathname) {
			String name = pathname.getName();
			return pathname.isFile() && !pathname.isHidden() && name.length() > suffix.length() && name.endsWith(suffix);
		}
	}
	
	public String getTriggerServerHost() {
		return triggerServerHost;
	}
	
	public int getTriggerServerPort() {
		return triggerServerPort;
	}
	
	public State getUpdaterThreadState() {
		return triggerManagingThread.getState();
	}
	
	public boolean isThreadActive() {
		return triggerManagingThread.isAlive();
	}
	
	public long getLastThreadCheckTime() {
		return lastThreadCheckTime;
	}
	
	public Set<String> getPrimaryServerHosts() {
		// Only one for now. More probably later.
		HashSet<String> ports = new HashSet<String>();
		ports.add(triggerServerHost + ":" + triggerServerPort);
		return ports;
	}
	
	private void loadTriggers() throws TriggerManagerException {
		List<Trigger> triggerList = triggerLoader.loadTriggers();
		for(Trigger t : triggerList) {
			triggerIdMap.put(t.getTriggerId(), t);
		}
	}
	
	public Trigger getTrigger(int triggerId) {
		return triggerIdMap.get(triggerId);
	}
	
	public void removeTrigger(Trigger t, String userId) throws TriggerManagerException {
		synchronized(t) {
			callTriggerServer(t, TriggerConnectorParams.REMOVE_TRIGGER_ACTION, userId);
		}
	}
	

	public void updateTrigger(Trigger t, String userId) throws TriggerManagerException {
		synchronized(t) {
			try {
				callTriggerServer(t, TriggerConnectorParams.UPDATE_TRIGGER_ACTION, userId);
			} catch(TriggerManagerException e) {
				throw new TriggerManagerException(e);
			}
		}
	}
	
//	public void getUpdatedTriggers() throws TriggerManagerException {
//		try {
//			callTriggerServer(triggerServerHost, triggerServerPort, TriggerConnectorParams.GET_UPDATE_ACTION, null, "azkaban", (Pair<String,String>[])null);
//		} catch(IOException e) {
//			throw new TriggerManagerException(e);
//		}
//	}
	
	public String insertTrigger(Trigger t, String userId) throws TriggerManagerException {
		synchronized(t) {
			String message = null;
			logger.info("Inserting trigger into system. " );
			// The trigger id is set by the loader. So it's unavailable until after this call.
			triggerLoader.addTrigger(t);
			try {
				callTriggerServer(t,  TriggerConnectorParams.INSERT_TRIGGER_ACTION, userId);
				triggerIdMap.put(t.getTriggerId(), t);
				
				message += "Trigger inserted successfully with trigger id " + t.getTriggerId();
			}
			catch (TriggerManagerException e) {
				throw e;
			}
			return message;
		}
	}
	
	private Map<String, Object> callTriggerServer(Trigger t, String action, String user) throws TriggerManagerException {
		try {
			Map<String, Object> info = t.getInfo();
			return callTriggerServer(triggerServerHost, triggerServerPort, action, t.getTriggerId(), null, (Pair<String,String>[])null);
		} catch (IOException e) {
			throw new TriggerManagerException(e);
		}
	}
	
	private Map<String, Object> callTriggerServer(String host, int port, String action, Integer triggerId, String user, Pair<String,String> ... params) throws IOException {
		URIBuilder builder = new URIBuilder();
		builder.setScheme("http")
			.setHost(host)
			.setPort(port)
			.setPath("/trigger");

		builder.setParameter(TriggerConnectorParams.ACTION_PARAM, action);
		
		if (triggerId != null) {
			builder.setParameter(TriggerConnectorParams.TRIGGER_ID_PARAM,String.valueOf(triggerId));
		}
		
		if (user != null) {
			builder.setParameter(TriggerConnectorParams.USER_PARAM, user);
		}
		
		if (params != null) {
			for (Pair<String, String> pair: params) {
				builder.setParameter(pair.getFirst(), pair.getSecond());
			}
		}

		URI uri = null;
		try {
			uri = builder.build();
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		
		ResponseHandler<String> responseHandler = new BasicResponseHandler();
		
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(uri);
		String response = null;
		try {
			response = httpclient.execute(httpget, responseHandler);
		} catch (IOException e) {
			throw e;
		}
		finally {
			httpclient.getConnectionManager().shutdown();
		}
		
		@SuppressWarnings("unchecked")
		Map<String, Object> jsonResponse = (Map<String, Object>)JSONUtils.parseJSONFromString(response);
		String error = (String)jsonResponse.get(TriggerConnectorParams.RESPONSE_ERROR);
		if (error != null) {
			throw new IOException(error);
		}
		
		return jsonResponse;
	}
	
	public Map<String, Object> callTriggerServerJMX(String hostPort, String action, String mBean) throws IOException {
		URIBuilder builder = new URIBuilder();
		
		String[] hostPortSplit = hostPort.split(":");
		builder.setScheme("http")
			.setHost(hostPortSplit[0])
			.setPort(Integer.parseInt(hostPortSplit[1]))
			.setPath("/jmx");

		builder.setParameter(action, "");
		if (mBean != null) {
			builder.setParameter(TriggerConnectorParams.JMX_MBEAN, mBean);
		}

		URI uri = null;
		try {
			uri = builder.build();
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		
		ResponseHandler<String> responseHandler = new BasicResponseHandler();
		
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(uri);
		String response = null;
		try {
			response = httpclient.execute(httpget, responseHandler);
		} catch (IOException e) {
			throw e;
		}
		finally {
			httpclient.getConnectionManager().shutdown();
		}
		
		@SuppressWarnings("unchecked")
		Map<String, Object> jsonResponse = (Map<String, Object>)JSONUtils.parseJSONFromString(response);
		String error = (String)jsonResponse.get(TriggerConnectorParams.RESPONSE_ERROR);
		if (error != null) {
			throw new IOException(error);
		}
		return jsonResponse;
	}
	
	public void shutdown() {
		triggerManagingThread.shutdown();
	}
	
	private class TriggerManagerUpdaterThread extends Thread {
		private boolean shutdown = false;

		public TriggerManagerUpdaterThread() {
			this.setName("TriggerManagingThread");
		}

		private int waitTimeIdleMs = 2000;
		private int waitTimeMs = 500;
		
		private void shutdown() {
			shutdown = true;
		}
		
		@SuppressWarnings("unchecked")
		public void run() {
			while(!shutdown) {
				try {
					lastThreadCheckTime = System.currentTimeMillis();
					
					Pair<String, Integer> triggerServer = (Pair<String, Integer>) triggerServers.toArray()[0];
					
					Pair<String, String> updateTimeParam = new Pair<String, String>("lastUpdateTime", String.valueOf(lastUpdateTime));
					Map<String, Object> results = null;
					try{
						results = callTriggerServer(triggerServer.getFirst(), triggerServer.getSecond(), TriggerConnectorParams.GET_UPDATE_ACTION, null, "azkaban", updateTimeParam);
//						lastUpdateTime = (Long) results.get(TriggerConnectorParams.RESPONSE_UPDATETIME);
						List<Integer> updates = (List<Integer>) results.get("updates");
						for(Integer update : updates) {
							Trigger t = triggerLoader.loadTrigger(update);
							lastUpdateTime = Math.max(lastUpdateTime, t.getLastModifyTime().getMillis());
							triggerIdMap.put(update, t);
						}
					} catch (Exception e) {
						logger.error(e);
						
					}
					
					synchronized(this) {
						try {
							if (triggerIdMap.size() > 0) {
								this.wait(waitTimeMs);
							}
							else {
								this.wait(waitTimeIdleMs);
							}
						} catch (InterruptedException e) {
						}
					}
				}
				catch (Exception e) {
					logger.error(e);
				}
			}
		}
	}
	
	private static class ConnectionInfo {
		private String host;
		private int port;

		public ConnectionInfo(String host, int port) {
			this.host = host;
			this.port = port;
		}

		@SuppressWarnings("unused")
		private ConnectionInfo getOuterType() {
			return ConnectionInfo.this;
		}
		
		public boolean isEqual(String host, int port) {
			return this.port == port && this.host.equals(host);
		}
		
		public String getHost() {
			return host;
		}
		
		public int getPort() {
			return port;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((host == null) ? 0 : host.hashCode());
			result = prime * result + port;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ConnectionInfo other = (ConnectionInfo) obj;
			if (host == null) {
				if (other.host != null)
					return false;
			} else if (!host.equals(other.host))
				return false;
			if (port != other.port)
				return false;
			return true;
		}
	}

	public void loadTriggerFromDir(File baseDir, Props props) throws Exception {
		File[] triggerFiles = baseDir.listFiles(new SuffixFilter(TRIGGER_SUFFIX));

		for(File triggerFile : triggerFiles) {
			Props triggerProps = new Props(props, triggerFile);
			String triggerType = triggerProps.getString("trigger.type");
			TriggerAgent agent = triggerAgents.get(triggerType);
			if(agent != null) {
				agent.loadTriggerFromProps(triggerProps);
			} else {
				throw new Exception("Trigger " + triggerType + " is not supported.");
			}
		}
	}

	public List<Trigger> getTriggers() {
		return new ArrayList<Trigger>(triggerIdMap.values());
	}

	public void expireTrigger(int triggerId) {
		// TODO Auto-generated method stub
		
	}

	public CheckerTypeLoader getCheckerLoader() {
		return checkerTypeLoader;
	}

	public ActionTypeLoader getActionLoader() {
		return actionTypeLoader;
	}

	public void addTriggerAgent(String triggerSource,
			TriggerAgent agent) {
		triggerAgents.put(triggerSource, agent);
	}

	public List<Trigger> getTriggers(String triggerSource) {
		List<Trigger> results = new ArrayList<Trigger>();
		for(Trigger t : triggerIdMap.values()) {
			if(t.getSource().equals(triggerSource)) {
				results.add(t);
			}
		}
		return results;
	}

	public List<Trigger> getUpdatedTriggers(String triggerSource, long lastUpdateTime) throws TriggerManagerException {
		getUpdatedTriggers();
		List<Trigger> triggers = new ArrayList<Trigger>();
		for(Trigger t : triggerIdMap.values()) {
			if(t.getSource().equals(triggerSource) && t.getLastModifyTime().getMillis() >= lastUpdateTime) {
				triggers.add(t);
			}
		}
		return triggers;
	}

	private void getUpdatedTriggers() throws TriggerManagerException {
		List<Trigger> triggers = triggerLoader.getUpdatedTriggers(this.lastUpdateTime);
		for(Trigger t : triggers) {
			this.lastUpdateTime = Math.max(this.lastUpdateTime, t.getLastModifyTime().getMillis());
			triggerIdMap.put(t.getTriggerId(), t);
		}
	}

	public void removeTrigger(int scheduleId, String submitUser) throws TriggerManagerException {
		removeTrigger(triggerIdMap.get(scheduleId), submitUser);
	}

	
}

