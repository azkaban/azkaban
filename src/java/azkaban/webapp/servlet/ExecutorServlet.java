package azkaban.webapp.servlet;

import java.io.IOException;
import java.util.HashMap;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import azkaban.executor.ExecutorManagerException;
import azkaban.executor.FlowRunnerManager;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanExecutorServer;
import azkaban.webapp.AzkabanWebServer;

public class ExecutorServlet extends HttpServlet {
	private static final Logger logger = Logger.getLogger(ExecutorServlet.class.getName());
	public static final String JSON_MIME_TYPE = "application/json";
	
	public enum State {
		FAILED, SUCCEEDED, RUNNING, WAITING, IGNORED, READY
	}
	private String sharedToken;
	private AzkabanExecutorServer application;
	private FlowRunnerManager flowRunnerManager;
	
	public ExecutorServlet(String token) {
		super();
		sharedToken = token;
	}
	
	@Override
	public void init(ServletConfig config) throws ServletException {
		application = (AzkabanExecutorServer) config.getServletContext().getAttribute(AzkabanServletContextListener.AZKABAN_SERVLET_CONTEXT_KEY);

		if (application == null) {
			throw new IllegalStateException(
					"No batch application is defined in the servlet context!");
		}

		flowRunnerManager = application.getFlowRunnerManager();
	}

	
	protected void writeJSON(HttpServletResponse resp, Object obj) throws IOException {
		resp.setContentType(JSON_MIME_TYPE);
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValue(resp.getOutputStream(), obj);
	}

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		HashMap<String,Object> respMap= new HashMap<String,Object>();
		
		String token = getParam(req, "sharedToken");
		if (!token.equals(sharedToken)) {
			respMap.put("error", "Mismatched token. Will not run.");
		}
		else if (!hasParam(req, "execid")) {
			respMap.put("error", "Parameter execid not set.");
		}
		else if (!hasParam(req, "execpath")) {
			respMap.put("error", "Parameter execpath not set.");
		}
		else {
			String execid = getParam(req, "execid");
			String execpath = getParam(req, "execpath");
			
			logger.info("Submitted " + execid + " with " + execpath);
			try {
				flowRunnerManager.submitFlow(execid, execpath);
			} catch (ExecutorManagerException e) {
				e.printStackTrace();
				respMap.put("error", e.getMessage());
			}
		}

		writeJSON(resp, respMap);
	}
	
	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		
	}
	
	/**
	 * Duplicated code with AbstractAzkabanServlet, but ne
	 */
	public boolean hasParam(HttpServletRequest request, String param) {
		return request.getParameter(param) != null;
	}

	public String getParam(HttpServletRequest request, String name)
			throws ServletException {
		String p = request.getParameter(name);
		if (p == null)
			throw new ServletException("Missing required parameter '" + name + "'.");
		else
			return p;
	}
}
