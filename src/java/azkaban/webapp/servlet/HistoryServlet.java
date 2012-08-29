package azkaban.webapp.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import azkaban.webapp.session.Session;

public class HistoryServlet extends LoginAbstractAzkabanServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
			Session session) throws ServletException, IOException {
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/historypage.vm");
		
		
		
		page.render();
	}

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
			Session session) throws ServletException, IOException {
		// TODO Auto-generated method stub
		
	}

}
