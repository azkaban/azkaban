package azkaban.webapp.servlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManager.ExecutionReference;
import azkaban.project.ProjectManager;
import azkaban.webapp.session.Session;

public class HistoryServlet extends LoginAbstractAzkabanServlet {

	private static final long serialVersionUID = 1L;
	private ProjectManager projectManager;
	private ExecutorManager executorManager;
	
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		projectManager = this.getApplication().getProjectManager();
		executorManager = this.getApplication().getExecutorManager();
	}

	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
			Session session) throws ServletException, IOException {
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/historypage.vm");
		int pageNum = getIntParam(req, "page", 1);
		int pageSize = getIntParam(req, "size", 16);
		
		if (pageNum < 0) {
			pageNum = 1;
		}
		
		List<ExecutionReference> history = executorManager.getFlowHistory(pageSize, (pageNum - 1)*pageSize);
		page.add("flowHistory", history);
		page.add("size", pageSize);
		page.add("page", pageNum);
		
		if (pageNum == 1) {
			page.add("previous", new PageSelection(1, pageSize, true, false));
		}
		page.add("next", new PageSelection(pageNum + 1, pageSize, false, false));
		// Now for the 5 other values.
		int pageStartValue = 1;
		if (pageNum > 3) {
			pageStartValue = pageNum - 2;
		}
		
		page.add("page1", new PageSelection(pageStartValue, pageSize, false, pageStartValue == pageNum));
		pageStartValue++;
		page.add("page2", new PageSelection(pageStartValue, pageSize, false, pageStartValue == pageNum));
		pageStartValue++;
		page.add("page3", new PageSelection(pageStartValue, pageSize, false, pageStartValue == pageNum));
		pageStartValue++;
		page.add("page4", new PageSelection(pageStartValue, pageSize, false, pageStartValue == pageNum));
		pageStartValue++;
		page.add("page5", new PageSelection(pageStartValue, pageSize, false, pageStartValue == pageNum));
		pageStartValue++;
		
		page.render();
	}

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
			Session session) throws ServletException, IOException {
		// TODO Auto-generated method stub
		
	}

	public class PageSelection {
		private int page;
		private int size;
		private boolean disabled;
		private boolean selected;
		
		public PageSelection(int page, int size, boolean disabled, boolean selected) {
			this.page = page;
			this.size = size;
			this.disabled = disabled;
			this.setSelected(selected);
		}
		
		public int getPage() {
			return page;
		}
		
		public int getSize() {
			return size;
		}
		
		public boolean getDisabled() {
			return disabled;
		}

		public boolean isSelected() {
			return selected;
		}

		public void setSelected(boolean selected) {
			this.selected = selected;
		}
	}
}
