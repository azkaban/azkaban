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
import java.util.Map;
import java.util.TreeMap;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.ObjectName;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.executor.ConnectorParams;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.server.session.Session;
import azkaban.trigger.TriggerManager;
import azkaban.user.Permission;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.webapp.AzkabanWebServer;

/**
 * Limited set of jmx calls for when you cannot attach to the jvm
 */
public class JMXHttpServlet extends LoginAbstractAzkabanServlet implements
    ConnectorParams {
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  private static final Logger logger = Logger.getLogger(JMXHttpServlet.class
      .getName());

  private UserManager userManager;
  private AzkabanWebServer server;
  private ExecutorManagerAdapter executorManager;
  private TriggerManager triggerManager;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);

    server = (AzkabanWebServer) getApplication();
    userManager = server.getUserManager();
    executorManager = server.getExecutorManager();

    triggerManager = server.getTriggerManager();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
    if (hasParam(req, "ajax")) {
      Map<String, Object> ret = new HashMap<String, Object>();

      if (!hasPermission(session.getUser(), Permission.Type.METRICS)) {
        ret.put("error", "User " + session.getUser().getUserId()
            + " has no permission.");
        this.writeJSON(resp, ret, true);
        return;
      }
      String ajax = getParam(req, "ajax");
      if (JMX_GET_ALL_EXECUTOR_ATTRIBUTES.equals(ajax)) {
        if (!hasParam(req, JMX_MBEAN) || !hasParam(req, JMX_HOSTPORT)) {
          ret.put("error", "Parameters '" + JMX_MBEAN + "' and '"
              + JMX_HOSTPORT + "' must be set");
          this.writeJSON(resp, ret, true);
          return;
        }

        String hostPort = getParam(req, JMX_HOSTPORT);
        String mbean = getParam(req, JMX_MBEAN);
        Map<String, Object> result =
            executorManager.callExecutorJMX(hostPort,
                JMX_GET_ALL_MBEAN_ATTRIBUTES, mbean);
        // order the attribute by name
        for (Map.Entry<String, Object> entry : result.entrySet()) {
          if (entry.getValue() instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> entryValue = (Map<String, Object>) entry.getValue();
            result.put(entry.getKey(), new TreeMap<String,Object>(entryValue));
          }
        }
        ret = result;
      } else if (JMX_GET_MBEANS.equals(ajax)) {
        ret.put("mbeans", server.getMbeanNames());
      } else if (JMX_GET_MBEAN_INFO.equals(ajax)) {
        if (hasParam(req, JMX_MBEAN)) {
          String mbeanName = getParam(req, JMX_MBEAN);
          try {
            ObjectName name = new ObjectName(mbeanName);
            MBeanInfo info = server.getMBeanInfo(name);
            ret.put("attributes", info.getAttributes());
            ret.put("description", info.getDescription());
          } catch (Exception e) {
            logger.error(e);
            ret.put("error", "'" + mbeanName + "' is not a valid mBean name");
          }
        } else {
          ret.put("error", "No 'mbean' name parameter specified");
        }
      } else if (JMX_GET_MBEAN_ATTRIBUTE.equals(ajax)) {
        if (!hasParam(req, JMX_MBEAN) || !hasParam(req, JMX_ATTRIBUTE)) {
          ret.put("error", "Parameters 'mbean' and 'attribute' must be set");
        } else {
          String mbeanName = getParam(req, JMX_MBEAN);
          String attribute = getParam(req, JMX_ATTRIBUTE);

          try {
            ObjectName name = new ObjectName(mbeanName);
            Object obj = server.getMBeanAttribute(name, attribute);
            ret.put("value", obj);
          } catch (Exception e) {
            logger.error(e);
            ret.put("error", "'" + mbeanName + "' is not a valid mBean name");
          }
        }
      } else if (JMX_GET_ALL_MBEAN_ATTRIBUTES.equals(ajax)) {
        if (!hasParam(req, JMX_MBEAN)) {
          ret.put("error", "Parameters 'mbean' must be set");
        } else {
          String mbeanName = getParam(req, JMX_MBEAN);
          try {
            ObjectName name = new ObjectName(mbeanName);
            MBeanInfo info = server.getMBeanInfo(name);

            MBeanAttributeInfo[] mbeanAttrs = info.getAttributes();
            Map<String, Object> attributes = new TreeMap<String, Object>();

            for (MBeanAttributeInfo attrInfo : mbeanAttrs) {
              Object obj = server.getMBeanAttribute(name, attrInfo.getName());
              attributes.put(attrInfo.getName(), obj);
            }

            ret.put("attributes", attributes);
          } catch (Exception e) {
            logger.error(e);
            ret.put("error", "'" + mbeanName + "' is not a valid mBean name");
          }
        }
      } else {
        ret.put("commands", new String[] {
            JMX_GET_MBEANS,
            JMX_GET_MBEAN_INFO + "&" + JMX_MBEAN + "=<name>",
            JMX_GET_MBEAN_ATTRIBUTE + "&" + JMX_MBEAN + "=<name>&"
                + JMX_ATTRIBUTE + "=<attributename>" });
      }
      this.writeJSON(resp, ret, true);
    } else {
      handleJMXPage(req, resp, session);
    }
  }

  private void handleJMXPage(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws IOException {
    Page page =
        newPage(req, resp, session,
            "azkaban/webapp/servlet/velocity/jmxpage.vm");

    if (!hasPermission(session.getUser(), Permission.Type.METRICS)) {
      page.add("errorMsg", "User " + session.getUser().getUserId()
          + " has no permission.");
      page.render();
      return;
    }

    page.add("mbeans", server.getMbeanNames());

    Map<String, Object> executorMBeans = new HashMap<String, Object>();
    for (String hostPort : executorManager.getAllActiveExecutorServerHosts()) {
      try {
        Map<String, Object> mbeans =
            executorManager.callExecutorJMX(hostPort, JMX_GET_MBEANS, null);

        executorMBeans.put(hostPort, mbeans.get("mbeans"));
      } catch (IOException e) {
        logger.error("Cannot contact executor " + hostPort, e);
      }
    }

    page.add("executorRemoteMBeans", executorMBeans);

    Map<String, Object> triggerserverMBeans = new HashMap<String, Object>();
    triggerserverMBeans.put(triggerManager.getJMX().getPrimaryServerHost(),
        triggerManager.getJMX().getAllJMXMbeans());

    page.add("triggerserverRemoteMBeans", triggerserverMBeans);

    page.render();
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {

  }

  protected boolean hasPermission(User user, Permission.Type type) {
    for (String roleName : user.getRoles()) {
      Role role = userManager.getRole(roleName);
      if (role.getPermission().isPermissionSet(type)
          || role.getPermission().isPermissionSet(Permission.Type.ADMIN)) {
        return true;
      }
    }

    return false;
  }
}
