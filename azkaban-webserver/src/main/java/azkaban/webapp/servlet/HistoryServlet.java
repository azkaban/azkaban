/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.webapp.servlet;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.joda.time.format.DateTimeFormat;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.server.session.Session;
import azkaban.webapp.AzkabanWebServer;

public class HistoryServlet extends LoginAbstractAzkabanServlet {

  private static final String FILTER_BY_DATE_PATTERN = "MM/dd/yyyy hh:mm aa";
  private static final long serialVersionUID = 1L;
  private ExecutorManagerAdapter executorManager;
  private ProjectManager projectManager;
  private ExecutorVMHelper vmHelper;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    AzkabanWebServer server = (AzkabanWebServer) getApplication();
    executorManager = server.getExecutorManager();
    projectManager = server.getProjectManager();
    vmHelper = new ExecutorVMHelper();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {

    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else if (hasParam(req, "days")) {
      handleHistoryDayPage(req, resp, session);
    } else if (hasParam(req, "timeline")) {
      handleHistoryTimelinePage(req, resp, session);
    } else {
      handleHistoryPage(req, resp, session);
    }
  }

  private void handleAJAXAction(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    HashMap<String, Object> ret = new HashMap<String, Object>();
    String ajaxName = getParam(req, "ajax");

    if (ajaxName.equals("fetch")) {
      fetchHistoryData(req, resp, ret);
    }

    if (ret != null) {
      this.writeJSON(resp, ret);
    }
  }

  private void fetchHistoryData(HttpServletRequest req,
      HttpServletResponse resp, HashMap<String, Object> ret)
      throws ServletException {
  }

  private void handleHistoryPage(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException {
    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/historypage.vm");
    int pageNum = getIntParam(req, "page", 1);
    int pageSize = getIntParam(req, "size", 16);
    page.add("vmutils", vmHelper);

    if (pageNum < 0) {
      pageNum = 1;
    }
    List<ExecutableFlow> history = null;
    if (hasParam(req, "advfilter")) {
      String projContain = getParam(req, "projcontain");
      String flowContain = getParam(req, "flowcontain");
      String userContain = getParam(req, "usercontain");
      int status = getIntParam(req, "status");
      String begin = getParam(req, "begin");

      long beginTime =
          begin == "" ? -1 : DateTimeFormat.forPattern(FILTER_BY_DATE_PATTERN)
              .parseDateTime(begin).getMillis();
      String end = getParam(req, "end");

      long endTime =
          end == "" ? -1 : DateTimeFormat.forPattern(FILTER_BY_DATE_PATTERN)
              .parseDateTime(end).getMillis();
      try {
        history =
            executorManager.getExecutableFlows(projContain, flowContain,
                userContain, status, beginTime, endTime, (pageNum - 1)
                    * pageSize, pageSize);
      } catch (ExecutorManagerException e) {
        page.add("error", e.getMessage());
      }
    } else if (hasParam(req, "search")) {
      String searchTerm = getParam(req, "searchterm");
      try {
        history =
            executorManager.getExecutableFlows(searchTerm, (pageNum - 1)
                * pageSize, pageSize);
      } catch (ExecutorManagerException e) {
        page.add("error", e.getMessage());
      }
    } else {
      try {
        history =
            executorManager.getExecutableFlows((pageNum - 1) * pageSize,
                pageSize);
      } catch (ExecutorManagerException e) {
        e.printStackTrace();
      }
    }
    page.add("flowHistory", history);
    page.add("size", pageSize);
    page.add("page", pageNum);
    // keep the search terms so that we can navigate to later pages
    if (hasParam(req, "searchterm") && !getParam(req, "searchterm").equals("")) {
      page.add("search", "true");
      page.add("search_term", getParam(req, "searchterm"));
    }

    if (hasParam(req, "advfilter")) {
      page.add("advfilter", "true");
      page.add("projcontain", getParam(req, "projcontain"));
      page.add("flowcontain", getParam(req, "flowcontain"));
      page.add("usercontain", getParam(req, "usercontain"));
      page.add("status", getIntParam(req, "status"));
      page.add("begin", getParam(req, "begin"));
      page.add("end", getParam(req, "end"));
    }

    if (pageNum == 1) {
      page.add("previous", new PageSelection(1, pageSize, true, false));
    } else {
      page.add("previous", new PageSelection(pageNum - 1, pageSize, false,
          false));
    }
    page.add("next", new PageSelection(pageNum + 1, pageSize, false, false));
    // Now for the 5 other values.
    int pageStartValue = 1;
    if (pageNum > 3) {
      pageStartValue = pageNum - 2;
    }

    page.add("page1", new PageSelection(pageStartValue, pageSize, false,
        pageStartValue == pageNum));
    pageStartValue++;
    page.add("page2", new PageSelection(pageStartValue, pageSize, false,
        pageStartValue == pageNum));
    pageStartValue++;
    page.add("page3", new PageSelection(pageStartValue, pageSize, false,
        pageStartValue == pageNum));
    pageStartValue++;
    page.add("page4", new PageSelection(pageStartValue, pageSize, false,
        pageStartValue == pageNum));
    pageStartValue++;
    page.add("page5", new PageSelection(pageStartValue, pageSize, false,
        pageStartValue == pageNum));
    pageStartValue++;

    page.render();
  }

  private void handleHistoryTimelinePage(HttpServletRequest req,
      HttpServletResponse resp, Session session) {
  }

  private void handleHistoryDayPage(HttpServletRequest req,
      HttpServletResponse resp, Session session) {
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

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
  }

  public class ExecutorVMHelper {
    public String getProjectName(int id) {
      Project project = projectManager.getProject(id);
      if (project == null) {
        return String.valueOf(id);
      }

      return project.getName();
    }
  }
}
