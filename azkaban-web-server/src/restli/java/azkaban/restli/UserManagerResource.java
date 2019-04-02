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

import azkaban.restli.user.User;
import azkaban.server.session.Session;
import azkaban.user.UserManager;
import azkaban.user.UserManagerException;
import azkaban.webapp.AzkabanWebServer;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiActions;
import com.linkedin.restli.server.resources.ResourceContextHolder;
import java.util.Set;
import java.util.UUID;
import javax.servlet.ServletException;
import org.apache.log4j.Logger;

@RestLiActions(name = "user", namespace = "azkaban.restli")
public class UserManagerResource extends ResourceContextHolder {

  private static final Logger logger = Logger
      .getLogger(UserManagerResource.class);

  public AzkabanWebServer getAzkaban() {
    return AzkabanWebServer.getInstance();
  }

  @Action(name = "login")
  public String login(@ActionParam("username") final String username,
      @ActionParam("password") final String password) throws UserManagerException,
      ServletException {
    final String ip = ResourceUtils.getRealClientIpAddr(this.getContext());
    logger
        .info("Attempting to login for " + username + " from ip '" + ip + "'");

    final Session session = createSession(username, password, ip);


    final Set<Session> sessionsOfSameIP = getAzkaban().getSessionCache()
        .findSessionsByIP(session.getIp());

    // Check potential DDoS attack by bad hosts.
    logger.info(
        "Session id created for user '" + username + "' and ip " + ip + ", " + sessionsOfSameIP
            .size() + " session(s) found from this IP " + session.getIp());

    return session.getSessionId();
  }

  @Action(name = "getUserFromSessionId")
  public User getUserFromSessionId(@ActionParam("sessionId") final String sessionId) {
    final Session session = getSessionFromSessionId(sessionId);
    final azkaban.user.User azUser = session.getUser();

    // Fill out the restli object with properties from the Azkaban user
    final User user = new User();
    user.setUserId(azUser.getUserId());
    user.setEmail(azUser.getEmail());
    return user;
  }

  private Session createSession(final String username, final String password, final String ip)
      throws UserManagerException {
    final UserManager manager = getAzkaban().getUserManager();
    final azkaban.user.User user = manager.getUser(username, password);

    final String randomUID = UUID.randomUUID().toString();
    final Session session = new Session(randomUID, user, ip);
    getAzkaban().getSessionCache().addSession(session);

    return session;
  }

  private Session getSessionFromSessionId(final String sessionId) {
    if (sessionId == null) {
      return null;
    }

    return getAzkaban().getSessionCache().getSession(sessionId);
  }
}
