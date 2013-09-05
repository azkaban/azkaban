package azkaban.trigger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;

import azkaban.execapp.AzkabanExecutorServer;
import azkaban.execapp.FlowRunnerManager;
import azkaban.executor.ConnectorParams;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerException;
import azkaban.triggerapp.AzkabanTriggerServer;
import azkaban.triggerapp.TriggerConnectorParams;
import azkaban.triggerapp.TriggerRunnerManagerException;
import azkaban.utils.JSONUtils;
import azkaban.utils.FileIOUtils.JobMetaData;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.webapp.servlet.AbstractServiceServlet;
import azkaban.webapp.servlet.AzkabanServletContextListener;

public class TriggerManagerServlet extends AbstractServiceServlet implements TriggerConnectorParams {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(TriggerManagerServlet.class.getName());
	public static final String JSON_MIME_TYPE = "application/json";

	private AzkabanTriggerServer application;
	private TriggerManager triggerManager;
	
	public static final String WEB_PATH = "/triggermanager";
	
	public TriggerManagerServlet() {
		super();
	}
	
	@Override
	public void init(ServletConfig config) throws ServletException {
		application = (AzkabanTriggerServer) config.getServletContext().getAttribute(AzkabanServletContextListener.AZKABAN_SERVLET_CONTEXT_KEY);

		if (application == null) {
			throw new IllegalStateException(
					"No batch application is defined in the servlet context!");
		}

		triggerManager = application.getTriggerManager();
	}
	
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		HashMap<String,Object> respMap= new HashMap<String,Object>();
		//logger.info("ExecutorServer called by " + req.getRemoteAddr());
		try {
			if (!hasParam(req, ACTION_PARAM)) {
				logger.error("Parameter action not set");
				respMap.put("error", "Parameter action not set");
			}
			else {
				String action = getParam(req, ACTION_PARAM);
				if (action.equals(GET_UPDATE_ACTION)) {
					//logger.info("Updated called");
					handleAjaxGetUpdateRequest(req, respMap);
				}
				else if (action.equals(PING_ACTION)) {
					respMap.put("status", "alive");
				}
				else {
					int triggerId = Integer.parseInt(getParam(req, TRIGGER_ID_PARAM));
					String user = getParam(req, USER_PARAM, null);
					
					logger.info("User " + user + " has called action " + action + " on " + triggerId);
					if (action.equals(INSERT_TRIGGER_ACTION)) {
						logger.info("Insert Trigger Action");
						handleInsertTrigger(triggerId, user, req, resp, respMap);
					} else if (action.equals(REMOVE_TRIGGER_ACTION)) {
						logger.info("Remove Trigger Action");
						handleRemoveTrigger(triggerId, user, req, resp, respMap);
					} 
					else if (action.equals(UPDATE_TRIGGER_ACTION)) {
						logger.info("Update Trigger Action");
						handleUpdateTrigger(triggerId, user, req, respMap);
					}
					else {
						logger.error("action: '" + action + "' not supported.");
						respMap.put("error", "action: '" + action + "' not supported.");
					}
				}
			}
		} catch (Exception e) {
			logger.error(e);
			respMap.put(RESPONSE_ERROR, e.getMessage());
		}
		writeJSON(resp, respMap);
		resp.flushBuffer();
	}
	
	

	private void handleAjaxGetUpdateRequest(HttpServletRequest req, HashMap<String, Object> respMap) {
		List<Integer> updates = null;
		try{
			long lastUpdateTime = getLongParam(req, "lastUpdateTime");
//			respMap.put(TriggerConnectorParams.RESPONSE_UPDATETIME, DateTime.now().getMillis());
			updates = triggerManager.getTriggerUpdates(lastUpdateTime);
			if(updates.size() > 0) {
				System.out.println("got " + updates.size() + " updates" );
			}
			respMap.put("updates", updates);
		} catch (Exception e) {
			logger.error(e);
			respMap.put("error", e.getMessage());
		}
	}

	private void handleInsertTrigger(int triggerId, String user, HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> respMap) {
		try {
			triggerManager.insertTrigger(triggerId, user);
		} catch (TriggerManagerException e) {
			logger.error(e);
			respMap.put("error", e.getMessage());
		}
	}
	
	private void handleUpdateTrigger(int triggerId, String user, HttpServletRequest req, HashMap<String, Object> respMap) {
		try {
			triggerManager.updateTrigger(triggerId, user);
		} catch (TriggerManagerException e) {
			logger.error(e);
			respMap.put("error", e.getMessage());
		}
	}

	private void handleRemoveTrigger(int triggerId, String user, HttpServletRequest req, HttpServletResponse resp, HashMap<String, Object> respMap) {
		try {
			triggerManager.removeTrigger(triggerId, user);
		} catch (TriggerManagerException e) {
			logger.error(e);
			respMap.put("error", e.getMessage());
		}
	}

	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		
	}

}
