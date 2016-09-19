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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import javax.servlet.ServletException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.project.validator.ValidationReport;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.user.UserManagerException;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import azkaban.webapp.AzkabanWebServer;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiActions;
import com.linkedin.restli.server.resources.ResourceContextHolder;
import com.linkedin.restli.server.RestLiServiceException;

@RestLiActions(name = "project", namespace = "azkaban.restli")
public class ProjectManagerResource extends ResourceContextHolder {
  private static final Logger logger = Logger
      .getLogger(ProjectManagerResource.class);

  public AzkabanWebServer getAzkaban() {
    return AzkabanWebServer.getInstance();
  }

  @Action(name = "deploy")
  public String deploy(@ActionParam("sessionId") String sessionId,
      @ActionParam("projectName") String projectName,
      @ActionParam("packageUrl") String packageUrl)
      throws ProjectManagerException, RestLiServiceException, UserManagerException,
      ServletException, IOException {
    logger.info("Deploy called. {sessionId: " + sessionId + ", projectName: "
        + projectName + ", packageUrl:" + packageUrl + "}");

    String ip =
        (String) this.getContext().getRawRequestContext()
            .getLocalAttr("REMOTE_ADDR");
    User user = ResourceUtils.getUserFromSessionId(sessionId, ip);
    ProjectManager projectManager = getAzkaban().getProjectManager();
    Project project = projectManager.getProject(projectName);
    if (project == null) {
      String errorMsg = "Project '" + projectName + "' not found.";
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, errorMsg);
    }

    if (!ResourceUtils.hasPermission(project, user, Permission.Type.WRITE)) {
      String errorMsg =
          "User " + user.getUserId()
              + " has no permission to write to project " + project.getName();
      logger.error(errorMsg);
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, errorMsg);
    }

    logger.info("Target package URL is " + packageUrl);
    URL url = null;
    try {
      url = new URL(packageUrl);
    } catch (MalformedURLException e) {
      String errorMsg = "URL " + packageUrl + " is malformed.";
      logger.error(errorMsg, e);
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, errorMsg);
    }

    String filename = getFileName(url.getFile());
    File tempDir = Utils.createTempDir();
    File archiveFile = new File(tempDir, filename);
    try {
      // Since zip files can be large, don't specify an explicit read or
      // connection
      // timeout. This will cause the call to block until the download is
      // complete.
      logger.info("Downloading package from " + packageUrl);
      FileUtils.copyURLToFile(url, archiveFile);

      logger.info("Downloaded to " + archiveFile.toString());
    } catch (IOException e) {
      String errorMsg =
          "Download of URL " + packageUrl + " to " + archiveFile.toString()
              + " failed";
      logger.error(errorMsg, e);
      if (tempDir.exists()) {
        FileUtils.deleteDirectory(tempDir);
      }
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, errorMsg, e);
    }

    try {
      // Check if project upload runs into any errors, such as the file
      // having blacklisted jars
      Props props = new Props();
      Map<String, ValidationReport> reports = projectManager.uploadProject(project, archiveFile, "zip", user, props);
      checkReports(reports);
      return Integer.toString(project.getVersion());
    } catch (ProjectManagerException e) {
      String errorMsg = "Upload of project " + project + " from " + archiveFile + " failed";
      logger.error(errorMsg, e);
      throw e;
    } finally {
      if (tempDir.exists()) {
        FileUtils.deleteDirectory(tempDir);
      }
    }
  }

  void checkReports(Map<String, ValidationReport> reports) throws RestLiServiceException {
    StringBuffer errorMsgs = new StringBuffer();
    for (Map.Entry<String, ValidationReport> reportEntry : reports.entrySet()) {
      ValidationReport report = reportEntry.getValue();
      if (!report.getErrorMsgs().isEmpty()) {
        errorMsgs.append("Validator " + reportEntry.getKey() + " reports errors: ");
        for (String msg : report.getErrorMsgs()) {
          errorMsgs.append(msg + System.getProperty("line.separator"));
        }
      }
    }
    if (errorMsgs.length() > 0) {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, errorMsgs.toString());
    }
  }

  private String getFileName(String file) {
    return file.substring(file.lastIndexOf("/") + 1);
  }
}
