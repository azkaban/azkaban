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

import azkaban.Constants.ConfigurationKeys;
import azkaban.executor.ExecutorManagerException;
import azkaban.flowtrigger.quartz.FlowTriggerScheduler;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.project.validator.ValidationReport;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.user.UserManagerException;
import azkaban.utils.Utils;
import azkaban.webapp.AzkabanWebServer;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiActions;
import com.linkedin.restli.server.resources.ResourceContextHolder;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import javax.servlet.ServletException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.quartz.SchedulerException;

@RestLiActions(name = "project", namespace = "azkaban.restli")
public class ProjectManagerResource extends ResourceContextHolder {

  private static final Logger logger = Logger
      .getLogger(ProjectManagerResource.class);

  public AzkabanWebServer getAzkaban() {
    return AzkabanWebServer.getInstance();
  }

  @Action(name = "deploy")
  public String deploy(@ActionParam("sessionId") final String sessionId,
      @ActionParam("projectName") final String projectName,
      @ActionParam("packageUrl") final String packageUrl)
      throws ProjectManagerException, RestLiServiceException, UserManagerException,
      ServletException, IOException, SchedulerException, ExecutorManagerException {
    logger.info("Deploy called. {projectName: " + projectName + ", packageUrl:" + packageUrl + "}");

    final String ip = ResourceUtils.getRealClientIpAddr(this.getContext());
    final User user = ResourceUtils.getUserFromSessionId(sessionId);
    final ProjectManager projectManager = getAzkaban().getProjectManager();
    final Project project = projectManager.getProject(projectName);

    final FlowTriggerScheduler scheduler = getAzkaban().getFlowTriggerScheduler();
    final boolean enableQuartz = getAzkaban().getServerProps().getBoolean(ConfigurationKeys
        .ENABLE_QUARTZ, false);

    logger.info("Deploy: reference of project " + projectName + " is " + System.identityHashCode
        (project));
    if (project == null) {
      final String errorMsg = "Project '" + projectName + "' not found.";
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, errorMsg);
    }

    if (!project.isActive()) {
      final String errorMsg =
          "Installation Failed. Project '" + projectName + "' was already removed.";
      throw new RestLiServiceException(HttpStatus.S_410_GONE, errorMsg);
    }

    if (!ResourceUtils.hasPermission(project, user, Permission.Type.WRITE)) {
      final String errorMsg =
          "User " + user.getUserId()
              + " has no permission to write to project " + project.getName();
      logger.error(errorMsg);
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, errorMsg);
    }

    logger.info("Target package URL is " + packageUrl);
    URL url = null;
    try {
      url = new URL(packageUrl);
    } catch (final MalformedURLException e) {
      final String errorMsg = "URL " + packageUrl + " is malformed.";
      logger.error(errorMsg, e);
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, errorMsg);
    }

    final String filename = getFileName(url.getFile());
    final File tempDir = Utils.createTempDir();
    final File archiveFile = new File(tempDir, filename);
    try {
      // Since zip files can be large, don't specify an explicit read or
      // connection
      // timeout. This will cause the call to block until the download is
      // complete.
      logger.info("Downloading package from " + packageUrl);
      FileUtils.copyURLToFile(url, archiveFile);

      logger.info("Downloaded to " + archiveFile.toString());
    } catch (final IOException e) {
      final String errorMsg =
          "Download of URL " + packageUrl + " to " + archiveFile.toString()
              + " failed";
      logger.error(errorMsg, e);
      if (tempDir.exists()) {
        FileUtils.deleteDirectory(tempDir);
      }
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, errorMsg, e);
    }

    try {
      if (enableQuartz) {
        //todo chengren311: should maintain atomicity,
        // e.g, if uploadProject fails, associated schedule shouldn't be added.
        scheduler.unschedule(project);
      }
      // Check if project upload runs into any errors, such as the file
      // having blacklisted jars
      final Map<String, ValidationReport> reports = projectManager
          .uploadProject(project, archiveFile, "zip", user, null, null);

      if (enableQuartz) {
        scheduler.schedule(project, user.getUserId());
      }

      checkReports(reports);
      logger.info("Deploy: project " + projectName + " version is " + project.getVersion()
          + ", reference is " + System.identityHashCode(project));
      return Integer.toString(project.getVersion());
    } catch (final ProjectManagerException | ExecutorManagerException e) {
      final String errorMsg = "Upload of project " + project + " from " + archiveFile + " failed";
      logger.error(errorMsg, e);
      throw e;
    } finally {
      if (tempDir.exists()) {
        FileUtils.deleteDirectory(tempDir);
      }
    }
  }

  void checkReports(final Map<String, ValidationReport> reports) throws RestLiServiceException {
    final StringBuffer errorMsgs = new StringBuffer();
    for (final Map.Entry<String, ValidationReport> reportEntry : reports.entrySet()) {
      final ValidationReport report = reportEntry.getValue();
      if (!report.getErrorMsgs().isEmpty()) {
        errorMsgs.append("Validator " + reportEntry.getKey() + " reports errors: ");
        for (final String msg : report.getErrorMsgs()) {
          errorMsgs.append(msg + System.getProperty("line.separator"));
        }
      }
    }
    if (errorMsgs.length() > 0) {
      throw new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, errorMsgs.toString());
    }
  }

  private String getFileName(final String file) {
    return file.substring(file.lastIndexOf("/") + 1);
  }
}
