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

package azkaban.reportal.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;

import org.apache.commons.lang.StringUtils;

import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.user.User;
import azkaban.webapp.AzkabanWebServer;

public class ReportalHelper {
  public static List<Project> getReportalProjects(AzkabanWebServer server) {
    List<Project> projects = server.getProjectManager().getProjects();

    List<Project> reportalProjects = new ArrayList<Project>();

    for (Project project : projects) {
      if (project.getMetadata().containsKey("reportal-user")) {
        reportalProjects.add(project);
      }
    }

    return reportalProjects;
  }

  public static void bookmarkProject(AzkabanWebServer server, Project project,
      User user) throws ProjectManagerException {
    project.getMetadata().put("bookmark-" + user.getUserId(), true);
    server.getProjectManager().updateProjectSetting(project);
  }

  public static void unBookmarkProject(AzkabanWebServer server,
      Project project, User user) throws ProjectManagerException {
    project.getMetadata().remove("bookmark-" + user.getUserId());
    server.getProjectManager().updateProjectSetting(project);
  }

  public static boolean isBookmarkProject(Project project, User user) {
    return project.getMetadata().containsKey("bookmark-" + user.getUserId());
  }

  public static void subscribeProject(AzkabanWebServer server, Project project,
      User user, String email) throws ProjectManagerException {
    @SuppressWarnings("unchecked")
    Map<String, String> subscription =
        (Map<String, String>) project.getMetadata().get("subscription");
    if (subscription == null) {
      subscription = new HashMap<String, String>();
    }

    if (email != null && !email.isEmpty()) {
      subscription.put(user.getUserId(), email);
    }

    project.getMetadata().put("subscription", subscription);
    updateProjectNotifications(project, server.getProjectManager());
    server.getProjectManager().updateProjectSetting(project);
  }

  public static void unSubscribeProject(AzkabanWebServer server,
      Project project, User user) throws ProjectManagerException {
    @SuppressWarnings("unchecked")
    Map<String, String> subscription =
        (Map<String, String>) project.getMetadata().get("subscription");
    if (subscription == null) {
      return;
    }
    subscription.remove(user.getUserId());
    project.getMetadata().put("subscription", subscription);
    updateProjectNotifications(project, server.getProjectManager());
    server.getProjectManager().updateProjectSetting(project);
  }

  public static boolean isSubscribeProject(Project project, User user) {
    @SuppressWarnings("unchecked")
    Map<String, String> subscription =
        (Map<String, String>) project.getMetadata().get("subscription");
    if (subscription == null) {
      return false;
    }
    return subscription.containsKey(user.getUserId());
  }

  /**
   * Updates the email notifications saved in the project's flow.
   *
   * @param project
   * @param pm
   * @throws ProjectManagerException
   */
  public static void updateProjectNotifications(Project project,
      ProjectManager pm) throws ProjectManagerException {
    Flow flow = project.getFlows().get(0);

    // Get all success emails.
    ArrayList<String> successEmails = new ArrayList<String>();
    String successNotifications =
        (String) project.getMetadata().get("notifications");
    String[] successEmailSplit =
        successNotifications.split("\\s*,\\s*|\\s*;\\s*|\\s+");
    successEmails.addAll(Arrays.asList(successEmailSplit));

    // Get all failure emails.
    ArrayList<String> failureEmails = new ArrayList<String>();
    String failureNotifications =
        (String) project.getMetadata().get("failureNotifications");
    String[] failureEmailSplit =
        failureNotifications.split("\\s*,\\s*|\\s*;\\s*|\\s+");
    failureEmails.addAll(Arrays.asList(failureEmailSplit));

    // Add subscription emails to success emails list.
    @SuppressWarnings("unchecked")
    Map<String, String> subscription =
        (Map<String, String>) project.getMetadata().get("subscription");
    if (subscription != null) {
      successEmails.addAll(subscription.values());
    }

    ArrayList<String> successEmailList = new ArrayList<String>();
    for (String email : successEmails) {
      if (!email.trim().isEmpty()) {
        successEmailList.add(email);
      }
    }

    ArrayList<String> failureEmailList = new ArrayList<String>();
    for (String email : failureEmails) {
      if (!email.trim().isEmpty()) {
        failureEmailList.add(email);
      }
    }

    // Save notifications in the flow.
    flow.getSuccessEmails().clear();
    flow.getFailureEmails().clear();
    flow.addSuccessEmails(successEmailList);
    flow.addFailureEmails(failureEmailList);
    pm.updateFlow(project, flow);
  }

  public static boolean isScheduledProject(Project project) {
    Object schedule = project.getMetadata().get("schedule");
    if (schedule == null || !(schedule instanceof Boolean)) {
      return false;
    }
    return (boolean) (Boolean) schedule;
  }

  public static boolean isScheduledRepeatingProject(Project project) {
    Object schedule = project.getMetadata().get("scheduleRepeat");
    if (schedule == null || !(schedule instanceof Boolean)) {
      return false;
    }
    return (boolean) (Boolean) schedule;
  }

  public static List<Project> getUserReportalProjects(AzkabanWebServer server,
      String userName) throws ProjectManagerException {
    ProjectManager projectManager = server.getProjectManager();
    List<Project> projects = projectManager.getProjects();
    List<Project> result = new ArrayList<Project>();

    for (Project project : projects) {
      if (userName.equals(project.getMetadata().get("reportal-user"))) {
        result.add(project);
      }
    }

    return result;
  }

  public static Project createReportalProject(AzkabanWebServer server,
      String title, String description, User user)
      throws ProjectManagerException {
    ProjectManager projectManager = server.getProjectManager();
    String projectName =
        "reportal-" + user.getUserId() + "-" + sanitizeText(title);
    Project project = projectManager.getProject(projectName);
    if (project != null) {
      return null;
    }
    project = projectManager.createProject(projectName, description, user);

    return project;
  }

  public static String sanitizeText(String text) {
    return text.replaceAll("[^A-Za-z0-9]", "-");
  }

  public static File findAvailableFileName(File parent, String name,
      String extension) {
    if (name.isEmpty()) {
      name = "untitled";
    }
    File file = new File(parent, name + extension);
    int i = 1;
    while (file.exists()) {
      file = new File(parent, name + "-" + i + extension);
      i++;
    }
    return file;
  }

  public static String prepareStringForJS(Object object) {
    return object.toString().replace("\r", "").replace("\n", "\\n");
  }

  public static String[] filterCSVFile(String[] files) {
    List<String> result = new ArrayList<String>();
    for (int i = 0; i < files.length; i++) {
      if (StringUtils.endsWithIgnoreCase(files[i], ".csv")) {
        result.add(files[i]);
      }
    }
    return result.toArray(new String[result.size()]);
  }

  /**
   * Given a string containing multiple emails, splits it based on the given
   * regular expression, and returns a set containing the unique, non-empty
   * emails.
   *
   * @param emailList
   * @return
   */
  public static Set<String> parseUniqueEmails(String emailList,
      String splitRegex) {
    Set<String> uniqueEmails = new HashSet<String>();

    if (emailList == null) {
      return uniqueEmails;
    }

    String[] emails = emailList.trim().split(splitRegex);
    for (String email : emails) {
      if (!email.isEmpty()) {
        uniqueEmails.add(email);
      }
    }

    return uniqueEmails;
  }

  /**
   * Returns true if the given email is valid and false otherwise.
   *
   * @param email
   * @return
   */
  public static boolean isValidEmailAddress(String email) {
    if (email == null) {
      return false;
    }

    boolean result = true;
    try {
      InternetAddress emailAddr = new InternetAddress(email);
      emailAddr.validate();
    } catch (AddressException ex) {
      result = false;
    }
    return result;
  }

  /**
   * Given an email string, returns the domain part if it exists, and null
   * otherwise.
   *
   * @param email
   * @return
   */
  public static String getEmailDomain(String email) {
    if (email == null || email.isEmpty()) {
      return null;
    }

    int atSignIndex = email.indexOf('@');
    if (atSignIndex != -1) {
      return email.substring(atSignIndex + 1);
    }

    return null;
  }
}
