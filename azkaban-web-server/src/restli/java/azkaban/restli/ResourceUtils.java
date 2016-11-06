/*
 * Copyright 2014 LinkedIn Corp.
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
package azkaban.restli;

import azkaban.project.Project;
import azkaban.user.Permission;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserManagerException;
import azkaban.utils.WebUtils;
import azkaban.webapp.AzkabanWebServer;
import azkaban.server.session.Session;
import com.linkedin.restli.server.ResourceContext;

import java.util.Map;

public class ResourceUtils {

  public static boolean hasPermission(Project project, User user,
      Permission.Type type) {
    UserManager userManager = AzkabanWebServer.getInstance().getUserManager();
    if (project.hasPermission(user, type)) {
      return true;
    }

    for (String roleName : user.getRoles()) {
      Role role = userManager.getRole(roleName);
      if (role.getPermission().isPermissionSet(type)
          || role.getPermission().isPermissionSet(Permission.Type.ADMIN)) {
        return true;
      }
    }

    return false;
  }

  public static User getUserFromSessionId(String sessionId, String ip)
      throws UserManagerException {
    Session session =
        AzkabanWebServer.getInstance().getSessionCache().getSession(sessionId);
    if (session == null) {
      throw new UserManagerException("Invalid session. Login required");
    } else if (!session.getIp().equals(ip)) {
      throw new UserManagerException("Invalid session. Session expired.");
    }

    return session.getUser();
  }

  public static String getRealClientIpAddr(ResourceContext context){

    // If some upstream device added an X-Forwarded-For header
    // use it for the client ip
    // This will support scenarios where load balancers or gateways
    // front the Azkaban web server and a changing Ip address invalidates
    // the session
    Map<String, String> headers = context.getRequestHeaders();

    WebUtils utils = new WebUtils();

    return utils.getRealClientIpAddr(headers,
            (String) context.getRawRequestContext().getLocalAttr("REMOTE_ADDR"));
  }
}
