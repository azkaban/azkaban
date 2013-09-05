package azkaban.trigger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;

import azkaban.triggerapp.TriggerConnectorParams;
import azkaban.triggerapp.TriggerRunnerManagerException;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;

public class TriggerManagerRemoteAdapter implements TriggerManagerAdapter{

	private final String host;
	private final int port;
	
	public TriggerManagerRemoteAdapter(Props props) {
		host = props.getString("trigger.server.host", "localhost");
		port = props.getInt("trigger.server.port");
	}
	
	@Override
	public void insertTrigger(Trigger t, String user) throws TriggerManagerException {
		try {
			callRemoteTriggerRunnerManager(TriggerConnectorParams.INSERT_TRIGGER_ACTION, t.getTriggerId(), user, (Pair<String,String>[])null);
		} catch(IOException e) {
			throw new TriggerManagerException(e);
		}
	}

	@Override
	public void removeTrigger(int id, String user) throws TriggerManagerException {
		try {
			callRemoteTriggerRunnerManager(TriggerConnectorParams.REMOVE_TRIGGER_ACTION, id, user, (Pair<String,String>[])null);
		} catch(IOException e) {
			throw new TriggerManagerException(e);
		}
	}

	@Override
	public void updateTrigger(int triggerId, String user) throws TriggerManagerException {
		try {
			callRemoteTriggerRunnerManager(TriggerConnectorParams.UPDATE_TRIGGER_ACTION, triggerId, user, (Pair<String,String>[])null);
		} catch(IOException e) {
			throw new TriggerManagerException(e);
		}
	}
	
	private Map<String, Object> callRemoteTriggerRunnerManager(String action, Integer triggerId, String user, Pair<String,String> ... params) throws IOException {
		URIBuilder builder = new URIBuilder();
		builder.setScheme("http")
			.setHost(host)
			.setPort(port)
			.setPath(TriggerManagerServlet.WEB_PATH);

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

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insertTrigger(int triggerId, String user) throws TriggerManagerException {
		try {
			callRemoteTriggerRunnerManager(TriggerConnectorParams.INSERT_TRIGGER_ACTION, triggerId, user, (Pair<String,String>[])null);
		} catch(IOException e) {
			throw new TriggerManagerException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Integer> getTriggerUpdates(long lastUpdateTime) throws TriggerManagerException {
		List<Integer> updated;
		try {
			Map<String, Object> response = callRemoteTriggerRunnerManager(TriggerConnectorParams.GET_UPDATE_ACTION, null, "azkaban", (Pair<String,String>[])null);
			updated = (List<Integer>) response.get(TriggerConnectorParams.RESPONSE_UPDATED_TRIGGERS);
			return updated;
		} catch(IOException e) {
			throw new TriggerManagerException(e);
		}
		
	}

	@Override
	public void updateTrigger(Trigger t, String user)
			throws TriggerManagerException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Trigger> getTriggerUpdates(String triggerSource,
			long lastUpdateTime) throws TriggerManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void registerCheckerType(String name, Class<? extends ConditionChecker> checker) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void registerActionType(String name,
			Class<? extends TriggerAction> action) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TriggerJMX getJMX() {
		// TODO Auto-generated method stub
		return null;
	}

}
