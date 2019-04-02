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
import azkaban.server.session.Session;
import azkaban.user.Permission;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserManagerException;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.WebUtils;
import com.linkedin.restli.server.ResourceContext;
import java.util.Map;

public class ResourceUtils {

  public static boolean hasPermission(final Project project, final User user,
      final Permission.Type type) {
    final UserManager userManager = AzkabanWebServer.getInstance().getUserManager();
    if (project.hasPermission(user, type)) {
      return true;
    }

    for (final String roleName : user.getRoles()) {
      final Role role = userManager.getRole(roleName);
      if (role.getPermission().isPermissionSet(type)
          || role.getPermission().isPermissionSet(Permission.Type.ADMIN)) {
        return true;
      }
    }

    return false;
  }

  public static User getUserFromSessionId(final String sessionId)
      throws UserManagerException {
    final Session session =
        AzkabanWebServer.getInstance().getSessionCache().getSession(sessionId);
    if (session == null) {
      throw new UserManagerException("Invalid session. Login required");
    }

    return session.getUser();
  }

  public static String getRealClientIpAddr(final ResourceContext context) {

    // If some upstream device added an X-Forwarded-For header
    // use it for the client ip
    // This will support scenarios where load balancers or gateways
    // front the Azkaban web server and a changing Ip address invalidates
    // the session
    final Map<String, String> headers = context.getRequestHeaders();

    return WebUtils.getRealClientIpAddr(headers,
        (String) context.getRawRequestContext().getLocalAttr("REMOTE_ADDR"));
  }
}
