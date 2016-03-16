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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.server.session.Session;
import azkaban.user.Permission;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.utils.Pair;
import azkaban.webapp.AzkabanWebServer;

/**
 * The main page
 */
public class ProjectServlet extends LoginAbstractAzkabanServlet {
  private static final Logger logger = Logger.getLogger(ProjectServlet.class
      .getName());
  private static final String LOCKDOWN_CREATE_PROJECTS_KEY =
      "lockdown.create.projects";
  private static final long serialVersionUID = -1;

  private UserManager userManager;

  private boolean lockdownCreateProjects = false;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    AzkabanWebServer server = (AzkabanWebServer) getApplication();

    userManager = server.getUserManager();
    lockdownCreateProjects =
        server.getServerProps().getBoolean(LOCKDOWN_CREATE_PROJECTS_KEY, false);
    if (lockdownCreateProjects) {
      logger.info("Creation of projects is locked down");
    }
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {

    ProjectManager manager =
        ((AzkabanWebServer) getApplication()).getProjectManager();

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
   * @SimplifiedProject object: information regarding projects, and information
   * regarding user and project association
   * 
   * @param req
   * @param resp
   * @param session
   * @param manager
   * @throws ServletException
   * @throws IOException
   */
  private void handleAjaxAction(HttpServletRequest req,
      HttpServletResponse resp, Session session, ProjectManager manager)
      throws ServletException, IOException {

    String ajaxName = getParam(req, "ajax");
    HashMap<String, Object> ret = new HashMap<String, Object>();

    if (ajaxName.equals("fetchallprojects")) {
      List<Project> projects = manager.getProjects();
      List<SimplifiedProject> simplifiedProjects =
          toSimplifiedProjects(projects);
      ret.put("projects", simplifiedProjects);
    } else if (ajaxName.equals("fetchuserprojects")) {
      handleFetchUserProjects(req, session, manager, ret);
    }

    this.writeJSON(resp, ret);
  }

  /**
   * We know the intention of API call is to return project ownership based on
   * given user. <br>
   * If user provides an user name, the method honors it <br>
   * If user provides an empty user name, the user defaults to the session user<br>
   * If user does not provide the user param, the user also defaults to the
   * session user<br>
   * 
   * @param req
   * @param session
   * @param manager
   * @param ret
   * @throws ServletException
   */
  private void handleFetchUserProjects(HttpServletRequest req, Session session,
      ProjectManager manager, HashMap<String, Object> ret)
      throws ServletException {
    User user = null;

    // if key "user" is specified, follow this logic
    if (hasParam(req, "user")) {
      String userParam = getParam(req, "user");
      if (userParam.isEmpty()) {
        user = session.getUser();
      } else {
        user = new User(userParam);
      }
    } else {
      // if key "user" is not specified, default to the session user
      user = session.getUser();
    }

    List<Project> projects = manager.getUserProjects(user);
    List<SimplifiedProject> simplifiedProjects = toSimplifiedProjects(projects);
    ret.put("projects", simplifiedProjects);
  }

  /**
   * A simple helper method that converts a List<Project> to List<SimplifiedProject>
   * 
   * @param projects
   * @return
   */
  private List<SimplifiedProject> toSimplifiedProjects(List<Project> projects) {
    List<SimplifiedProject> simplifiedProjects = new ArrayList<>();
    for (Project p : projects) {
      SimplifiedProject sp =
          new SimplifiedProject(p.getId(), p.getName(),
              p.getLastModifiedUser(), p.getCreateTimestamp(),
              p.getUserPermissions(), p.getGroupPermissions());
      simplifiedProjects.add(sp);
    }
    return simplifiedProjects;
  }

  /**
   * Renders the user homepage that users see when they log in
   * 
   * @param req
   * @param resp
   * @param session
   * @param manager
   */
  private void handlePageRender(HttpServletRequest req,
      HttpServletResponse resp, Session session, ProjectManager manager) {
    User user = session.getUser();

    Page page =
        newPage(req, resp, session, "azkaban/webapp/servlet/velocity/index.vm");

    if (lockdownCreateProjects && !hasPermissionToCreateProject(user)) {
      page.add("hideCreateProject", true);
    }

    if (hasParam(req, "all")) {
      List<Project> projects = manager.getProjects();
      page.add("viewProjects", "all");
      page.add("projects", projects);
    } else if (hasParam(req, "group")) {
      List<Project> projects = manager.getGroupProjects(user);
      page.add("viewProjects", "group");
      page.add("projects", projects);
    } else {
      List<Project> projects = manager.getUserProjects(user);
      page.add("viewProjects", "personal");
      page.add("projects", projects);
    }

    page.render();
  }

  private void handleDoAction(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException {
    if (getParam(req, "doaction").equals("search")) {
      String searchTerm = getParam(req, "searchterm");
      if (!searchTerm.equals("") && !searchTerm.equals(".*")) {
        handleFilter(req, resp, session, searchTerm);
        return;
      }
    }
  }

  private void handleFilter(HttpServletRequest req, HttpServletResponse resp,
      Session session, String searchTerm) {
    User user = session.getUser();
    ProjectManager manager =
        ((AzkabanWebServer) getApplication()).getProjectManager();
    Page page =
        newPage(req, resp, session, "azkaban/webapp/servlet/velocity/index.vm");
    if (hasParam(req, "all")) {
      // do nothing special if one asks for 'ALL' projects
      List<Project> projects = manager.getProjectsByRegex(searchTerm);
      page.add("allProjects", "");
      page.add("projects", projects);
      page.add("search_term", searchTerm);
    } else {
      List<Project> projects = manager.getUserProjectsByRegex(user, searchTerm);
      page.add("projects", projects);
      page.add("search_term", searchTerm);
    }

    page.render();
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
    // TODO Auto-generated method stub
  }

  private boolean hasPermissionToCreateProject(User user) {
    for (String roleName : user.getRoles()) {
      Role role = userManager.getRole(roleName);
      Permission perm = role.getPermission();
      if (perm.isPermissionSet(Permission.Type.ADMIN)
          || perm.isPermissionSet(Permission.Type.CREATEPROJECTS)) {
        return true;
      }
    }

    return false;
  }

  /**
   * This class is used to represent a simplified project, which can be returned
   * to end users via REST API. This is done in consideration that the API
   * caller only wants certain project level information regarding a project,
   * but does not want every flow and every job inside that project.
   * 
   * @author jyu
   *
   */
  private class SimplifiedProject {
    private int projectId;
    private String projectName;
    private String createdBy;
    private long createdTime;
    private List<Pair<String, Permission>> userPermissions;
    private List<Pair<String, Permission>> groupPermissions;

    public SimplifiedProject(int projectId, String projectName,
        String createdBy, long createdTime,
        List<Pair<String, Permission>> userPermissions,
        List<Pair<String, Permission>> groupPermissions) {
      this.projectId = projectId;
      this.projectName = projectName;
      this.createdBy = createdBy;
      this.createdTime = createdTime;
      this.userPermissions = userPermissions;
      this.groupPermissions = groupPermissions;
    }

    public int getProjectId() {
      return projectId;
    }

    public void setProjectId(int projectId) {
      this.projectId = projectId;
    }

    public String getProjectName() {
      return projectName;
    }

    public void setProjectName(String projectName) {
      this.projectName = projectName;
    }

    public String getCreatedBy() {
      return createdBy;
    }

    public void setCreatedBy(String createdBy) {
      this.createdBy = createdBy;
    }

    public long getCreatedTime() {
      return createdTime;
    }

    public void setCreatedTime(long createdTime) {
      this.createdTime = createdTime;
    }

    public List<Pair<String, Permission>> getUserPermissions() {
      return userPermissions;
    }

    public void setUserPermissions(
        List<Pair<String, Permission>> userPermissions) {
      this.userPermissions = userPermissions;
    }

    public List<Pair<String, Permission>> getGroupPermissions() {
      return groupPermissions;
    }

    public void setGroupPermissions(
        List<Pair<String, Permission>> groupPermissions) {
      this.groupPermissions = groupPermissions;
    }

  }
}
