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

import azkaban.Constants.ConfigurationKeys;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.server.session.Session;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserUtils;
import azkaban.utils.Pair;
import azkaban.webapp.AzkabanWebServer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

/**
 * The main page
 */
public class ProjectServlet extends LoginAbstractAzkabanServlet {

  private static final Logger logger = Logger.getLogger(ProjectServlet.class.getName());

  private static final long serialVersionUID = -1;

  private UserManager userManager;

  private boolean lockdownCreateProjects = false;

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = getApplication();

    this.userManager = server.getUserManager();
    this.lockdownCreateProjects =
        server.getServerProps().getBoolean(ConfigurationKeys.LOCKDOWN_CREATE_PROJECTS_KEY, false);
    if (this.lockdownCreateProjects) {
      logger.info("Creation of projects is locked down");
    }
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {

    final ProjectManager manager = getApplication().getProjectManager();

    if (hasParam(req, "ajax")) {
      handleAjaxAction(req, resp, session, manager);
    } else if (hasParam(req, "doaction")) {
      handleDoAction(req, resp, session);
    } else {
      handlePageRender(req, resp, session, manager);
    }
  }

  /**
   * ProjectServlet class now handles ajax requests. It returns a
   *
   * @SimplifiedProject object: information regarding projects, and information regarding user and
   * project association
   */
  private void handleAjaxAction(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session, final ProjectManager manager)
      throws ServletException, IOException {

    final String ajaxName = getParam(req, "ajax");
    final HashMap<String, Object> ret = new HashMap<>();

    if (ajaxName.equals("fetchallprojects")) {
      final List<Project> projects = manager.getProjects();
      final List<SimplifiedProject> simplifiedProjects =
          toSimplifiedProjects(projects);
      ret.put("projects", simplifiedProjects);
    } else if (ajaxName.equals("fetchuserprojects")) {
      handleFetchUserProjects(req, session, manager, ret);
    }

    this.writeJSON(resp, ret);
  }

  /**
   * We know the intention of API call is to return project ownership based on given user. <br> If
   * user provides an user name, the method honors it <br> If user provides an empty user name, the
   * user defaults to the session user<br> If user does not provide the user param, the user also
   * defaults to the session user<br>
   */
  private void handleFetchUserProjects(final HttpServletRequest req, final Session session,
      final ProjectManager manager, final HashMap<String, Object> ret)
      throws ServletException {
    User user = null;

    // if key "user" is specified, follow this logic
    if (hasParam(req, "user")) {
      final String userParam = getParam(req, "user");
      if (userParam.isEmpty()) {
        user = session.getUser();
      } else {
        user = new User(userParam);
      }
    } else {
      // if key "user" is not specified, default to the session user
      user = session.getUser();
    }

    final List<Project> projects = manager.getUserProjects(user);
    final List<SimplifiedProject> simplifiedProjects = toSimplifiedProjects(projects);
    ret.put("projects", simplifiedProjects);
  }

  /**
   * A simple helper method that converts a List<Project> to List<SimplifiedProject>
   */
  private List<SimplifiedProject> toSimplifiedProjects(final List<Project> projects) {
    final List<SimplifiedProject> simplifiedProjects = new ArrayList<>();
    for (final Project p : projects) {
      final SimplifiedProject sp =
          new SimplifiedProject(p.getId(), p.getName(),
              p.getLastModifiedUser(), p.getCreateTimestamp(),
              p.getUserPermissions(), p.getGroupPermissions());
      simplifiedProjects.add(sp);
    }
    return simplifiedProjects;
  }

  /**
   * Renders the user homepage that users see when they log in
   */
  private void handlePageRender(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session, final ProjectManager manager) {
    final User user = session.getUser();

    final Page page =
        newPage(req, resp, session, "azkaban/webapp/servlet/velocity/index.vm");

    if (this.lockdownCreateProjects &&
        !UserUtils.hasPermissionforAction(this.userManager, user, Permission.Type.CREATEPROJECTS)) {
      page.add("hideCreateProject", true);
    }

    if (hasParam(req, "all")) {
      final List<Project> projects = manager.getProjects();
      page.add("viewProjects", "all");
      page.add("projects", projects);
    } else if (hasParam(req, "group")) {
      final List<Project> projects = manager.getGroupProjects(user);
      page.add("viewProjects", "group");
      page.add("projects", projects);
    } else {
      final List<Project> projects = manager.getUserProjects(user);
      page.add("viewProjects", "personal");
      page.add("projects", projects);
    }

    page.render();
  }

  private void handleDoAction(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException {
    if (getParam(req, "doaction").equals("search")) {
      final String searchTerm = getParam(req, "searchterm");
      if (!searchTerm.equals("") && !searchTerm.equals(".*")) {
        handleFilter(req, resp, session, searchTerm);
        return;
      }
    }
  }

  private void handleFilter(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session, final String searchTerm) {
    final User user = session.getUser();
    final ProjectManager manager = getApplication().getProjectManager();
    final Page page =
        newPage(req, resp, session, "azkaban/webapp/servlet/velocity/index.vm");
    if (hasParam(req, "all")) {
      // do nothing special if one asks for 'ALL' projects
      final List<Project> projects = manager.getProjectsByRegex(searchTerm);
      page.add("allProjects", "");
      page.add("projects", projects);
      page.add("search_term", searchTerm);
    } else {
      final List<Project> projects = manager.getUserProjectsByRegex(user, searchTerm);
      page.add("projects", projects);
      page.add("search_term", searchTerm);
    }

    page.render();
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    // TODO Auto-generated method stub
  }

  /**
   * This class is used to represent a simplified project, which can be returned to end users via
   * REST API. This is done in consideration that the API caller only wants certain project level
   * information regarding a project, but does not want every flow and every job inside that
   * project.
   *
   * @author jyu
   */
  private static class SimplifiedProject {

    private int projectId;
    private String projectName;
    private String createdBy;
    private long createdTime;
    private List<Pair<String, Permission>> userPermissions;
    private List<Pair<String, Permission>> groupPermissions;

    public SimplifiedProject(final int projectId, final String projectName,
        final String createdBy, final long createdTime,
        final List<Pair<String, Permission>> userPermissions,
        final List<Pair<String, Permission>> groupPermissions) {
      this.projectId = projectId;
      this.projectName = projectName;
      this.createdBy = createdBy;
      this.createdTime = createdTime;
      this.userPermissions = userPermissions;
      this.groupPermissions = groupPermissions;
    }

    public int getProjectId() {
      return this.projectId;
    }

    public void setProjectId(final int projectId) {
      this.projectId = projectId;
    }

    public String getProjectName() {
      return this.projectName;
    }

    public void setProjectName(final String projectName) {
      this.projectName = projectName;
    }

    public String getCreatedBy() {
      return this.createdBy;
    }

    public void setCreatedBy(final String createdBy) {
      this.createdBy = createdBy;
    }

    public long getCreatedTime() {
      return this.createdTime;
    }

    public void setCreatedTime(final long createdTime) {
      this.createdTime = createdTime;
    }

    public List<Pair<String, Permission>> getUserPermissions() {
      return this.userPermissions;
    }

    public void setUserPermissions(
        final List<Pair<String, Permission>> userPermissions) {
      this.userPermissions = userPermissions;
    }

    public List<Pair<String, Permission>> getGroupPermissions() {
      return this.groupPermissions;
    }

    public void setGroupPermissions(
        final List<Pair<String, Permission>> groupPermissions) {
      this.groupPermissions = groupPermissions;
    }

  }
}
