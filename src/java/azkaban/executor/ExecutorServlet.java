package azkaban.executor;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

public class ExecutorServlet extends HttpServlet {
	private static final Logger logger = Logger.getLogger(ExecutorServlet.class.getName());
	private String sharedToken;
	
	public ExecutorServlet(String token) {
		super();
		sharedToken = token;
	}
	
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String token = getParam(req, "sharedToken");

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
			throw new ServletException("Missing required parameter '" + name
					+ "'.");
		else
			return p;
	}
}
