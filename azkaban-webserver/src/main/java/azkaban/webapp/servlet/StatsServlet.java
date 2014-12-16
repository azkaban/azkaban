package azkaban.webapp.servlet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import azkaban.executor.ConnectorParams;
import azkaban.executor.ExecutorManager;
import azkaban.server.session.Session;
import azkaban.user.Permission;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.utils.Pair;
import azkaban.webapp.AzkabanWebServer;


public class StatsServlet extends LoginAbstractAzkabanServlet {
  private static final long serialVersionUID = 1L;
  private UserManager userManager;
  private ExecutorManager execManager;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    AzkabanWebServer server = (AzkabanWebServer) getApplication();
    userManager = server.getUserManager();
    execManager = server.getExecutorManager();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    if (hasParam(req, ConnectorParams.ACTION_PARAM)) {
      handleAJAXAction(req, resp, session);
    } else {
      handleStatePageLoad(req, resp, session);
    }
  }

  private void handleAJAXAction(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    HashMap<String, Object> ret = new HashMap<String, Object>();
    String actionName = getParam(req, ConnectorParams.ACTION_PARAM);

    if (actionName.equals(ConnectorParams.STATS_GET_METRICHISTORY)) {
      handleGetMetricHistory(req, ret, session.getUser());
    } else if (actionName.equals(ConnectorParams.STATS_SET_REPORTINGINTERVAL)) {
      handleChangeMetricInterval(req, ret, session);
    }

    writeJSON(resp, ret);
  }

  private void handleChangeMetricInterval(HttpServletRequest req, HashMap<String, Object> ret, Session session)
      throws ServletException, IOException {
    try {
      Map<String, Object> result =
          execManager.callExecutorStats(ConnectorParams.STATS_GET_ALLMETRICSNAME, getAllParams(req));

      if (result.containsKey(ConnectorParams.RESPONSE_ERROR)) {
        throw new Exception(result.get(ConnectorParams.RESPONSE_ERROR).toString());
      }
      ret.put(ConnectorParams.STATUS_PARAM, result.get(ConnectorParams.STATUS_PARAM));
    } catch (Exception e) {
      ret.put(ConnectorParams.RESPONSE_ERROR, e.toString());
    }
  }

  private void handleGetMetricHistory(HttpServletRequest req, HashMap<String, Object> ret, User user)
      throws IOException, ServletException {
    try {
      Map<String, Object> result =
          execManager.callExecutorStats(ConnectorParams.STATS_GET_METRICHISTORY, getAllParams(req));
      if (result.containsKey(ConnectorParams.RESPONSE_ERROR)) {
        throw new Exception(result.get(ConnectorParams.RESPONSE_ERROR).toString());
      }
      ret.put("data", result.get("data"));
    } catch (Exception e) {
      ret.put(ConnectorParams.RESPONSE_ERROR, e.toString());
    }
  }

  private void handleStatePageLoad(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException {
    Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/statsPage.vm");
    if (!hasPermission(session.getUser(), Permission.Type.METRICS)) {
      page.add("errorMsg", "User " + session.getUser().getUserId() + " has no permission.");
      page.render();
      return;
    }
    try {
      Map<String, Object> result =
          execManager.callExecutorStats(ConnectorParams.STATS_GET_ALLMETRICSNAME, (Pair<String, String>[]) null);
      if (result.containsKey(ConnectorParams.RESPONSE_ERROR)) {
        throw new Exception(result.get(ConnectorParams.RESPONSE_ERROR).toString());
      }
      page.add("metricList", result.get("data"));
    } catch (Exception e) {
      page.add("errorMsg", e.toString());
    }
    page.render();
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException,
      IOException {
  }

  protected boolean hasPermission(User user, Permission.Type type) {
    for (String roleName : user.getRoles()) {
      Role role = userManager.getRole(roleName);
      if (role.getPermission().isPermissionSet(type) || role.getPermission().isPermissionSet(Permission.Type.ADMIN)) {
        return true;
      }
    }

    return false;
  }

  @SuppressWarnings("unchecked")
  private Pair<String, String>[] getAllParams(HttpServletRequest req) {
    List<Pair<String, String>> allParams = new LinkedList<Pair<String, String>>();

    Iterator it = req.getParameterMap().entrySet().iterator();
    while (it.hasNext()) {
        Map.Entry pairs = (Map.Entry)it.next();
        for(Object value : (String [])pairs.getValue()) {
          allParams.add(new Pair<String, String>((String) pairs.getKey(), (String) value));
        }
    }

    return allParams.toArray(new Pair[allParams.size()]);
  }
}
