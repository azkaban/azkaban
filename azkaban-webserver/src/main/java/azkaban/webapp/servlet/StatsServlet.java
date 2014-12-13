package azkaban.webapp.servlet;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import azkaban.metric.IMetric;
import azkaban.metric.IMetricEmitter;
import azkaban.metric.InMemoryHistoryNode;
import azkaban.metric.InMemoryMetricEmitter;
import azkaban.metric.MetricReportManager;
import azkaban.metric.TimeBasedReportingMetric;
import azkaban.server.session.Session;
import azkaban.user.Permission;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.webapp.AzkabanWebServer;


public class StatsServlet extends LoginAbstractAzkabanServlet {
  private UserManager userManager;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    AzkabanWebServer server = (AzkabanWebServer) getApplication();
    userManager = server.getUserManager();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException,
      IOException {
    if (hasParam(req, "ajax")) {
      handleAJAXAction(req, resp, session);
    } else if (hasParam(req, "action")) {
      String action = getParam(req, "action");
      if (action.equals("changeMetricInterval")) {
        handleChangeMetricInterval(req, resp, session);
      }
    } else {
      handleStatePageLoad(req, resp, session);
    }
  }

  private void handleChangeMetricInterval(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException {
    String metricName = getParam(req, "metricName");
    long newInterval = getLongParam(req, "interval");
    if(MetricReportManager.isInstantiated()) {
      MetricReportManager metricManager = MetricReportManager.getInstance();
      TimeBasedReportingMetric<?> metric = (TimeBasedReportingMetric<?>) metricManager.getMetricFromName(metricName);
      metric.updateInterval(newInterval);
    }
  }

  private void handleAJAXAction(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    HashMap<String, Object> ret = new HashMap<String, Object>();
    String ajaxName = getParam(req, "ajax");

    if (ajaxName.equals("metricHistory")) {
      getMetricHistory(req, ret, session.getUser());
    }

    if (ret != null) {
      this.writeJSON(resp, ret);
    }
  }

  private void getMetricHistory(HttpServletRequest req, HashMap<String, Object> ret, User user) throws ServletException {
    if (MetricReportManager.isInstantiated()) {
      MetricReportManager metricManager = MetricReportManager.getInstance();
      InMemoryMetricEmitter memoryEmitter = null;

      for (IMetricEmitter emitter : metricManager.getMetricEmitters()) {
        if (emitter instanceof InMemoryMetricEmitter) {
          memoryEmitter = (InMemoryMetricEmitter) emitter;
          break;
        }
      }

      // if we have a memory emitter
      if (memoryEmitter != null) {
        try {
          List<InMemoryHistoryNode> result =
              memoryEmitter.getDrawMetric(getParam(req, "metricName"), parseDate(getParam(req, "from")),
                  parseDate(getParam(req, "to")));
          if (result.size() > 0) {
            ret.put("data", result);
          } else {
            ret.put("error", "No metric stats available");
          }

        } catch (ParseException ex) {
          ret.put("error", "Invalid Date filter");
        }
      }
    }
  }

  private void handleStatePageLoad(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException {
    Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/statsPage.vm");
    MetricReportManager metricManager = MetricReportManager.getInstance();
    if (!hasPermission(session.getUser(), Permission.Type.METRICS)) {
      page.add("errorMsg", "User " + session.getUser().getUserId() + " has no permission.");
      page.render();
      return;
    }
    page.add("metricList", metricManager.getAllMetrics());
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

  private Date parseDate(String date) throws ParseException {
    DateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm a");
    return format.parse(date);
  }
}
