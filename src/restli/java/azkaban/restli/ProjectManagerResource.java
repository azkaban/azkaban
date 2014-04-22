package azkaban.restli;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;

import javax.servlet.ServletException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.user.UserManagerException;
import azkaban.utils.Utils;
import azkaban.webapp.AzkabanWebServer;

import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiActions;
import com.linkedin.restli.server.resources.ResourceContextHolder;

@RestLiActions(name = "project", namespace = "azkaban.restli")
public class ProjectManagerResource extends ResourceContextHolder {
	private static final Logger logger = Logger.getLogger(ProjectManagerResource.class);
	
	public AzkabanWebServer getAzkaban() {
		return AzkabanWebServer.getInstance();
	}
	
	@Action(name = "deploy")
	public String deploy(
			@ActionParam("sessionId") String sessionId,
			@ActionParam("projectName") String projectName,
			@ActionParam("packageUrl") String packageUrl)
			throws ProjectManagerException, UserManagerException, ServletException, IOException {
		logger.info("Deploy called. {sessionId: " + sessionId +
				", projectName: " + projectName + 
				", packageUrl:" + packageUrl + "}");
		
		String ip = (String)this.getContext().getRawRequestContext().getLocalAttr("REMOTE_ADDR");
		User user = ResourceUtils.getUserFromSessionId(sessionId, ip);
		ProjectManager projectManager = getAzkaban().getProjectManager();
		Project project = projectManager.getProject(projectName);
		if (project == null) {
			throw new ProjectManagerException("Project '" + projectName + "' not found.");
		}
		
		if (!ResourceUtils.hasPermission(project, user, Permission.Type.WRITE)) {
			String errorMsg = "User " + user.getUserId() + " has no permission to write to project " + project.getName();
			logger.error(errorMsg);
			throw new ProjectManagerException(errorMsg);
		}

		// Deploy stuff here. Move the code to a more formal area later.
		logger.info("Downloading file from " + packageUrl);
		URL url = null;
		InputStream urlFileInputStream = null;
		try {
			url = new URL(packageUrl);
			InputStream in = url.openStream();
			urlFileInputStream = new BufferedInputStream(in);
		} catch (MalformedURLException e) {
			String errorMsg = "Url " + packageUrl + " is malformed.";
			logger.error(errorMsg, e);
			throw new ProjectManagerException(errorMsg, e);
		} catch (IOException e) {
			String errorMsg = "Error opening input stream to " + packageUrl;
			logger.error(errorMsg, e);
			throw new ProjectManagerException(errorMsg, e);
		}
		
		String filename = getFileName(url.getFile());

		File tempDir = Utils.createTempDir();
		OutputStream fileOutputStream = null;
		try {
			logger.error("Downloading " + filename);
			File archiveFile = new File(tempDir, filename);
			fileOutputStream = new BufferedOutputStream(new FileOutputStream(archiveFile));
			IOUtils.copy(urlFileInputStream, fileOutputStream);
			fileOutputStream.close();
			
			logger.error("Downloaded to " + archiveFile.toString() + " " + archiveFile.length() + " bytes.");
			projectManager.uploadProject(project, archiveFile, "zip", user);
		} catch (Exception e) {
			logger.info("Installation Failed.", e);
			String error = e.getMessage();
			if (error.length() > 512) {
				error = error.substring(0, 512) + "\nToo many errors to display.\n";
			}
			
			throw new ProjectManagerException("Installation failed: " + error);
		}
		finally {
			if (tempDir.exists()) {
				FileUtils.deleteDirectory(tempDir);
			}
			if (urlFileInputStream != null) {
				urlFileInputStream.close();
			}
			if (fileOutputStream != null) {
				fileOutputStream.close();
			}
		}
		
		return Integer.toString(project.getVersion());
	}

	private String getFileName(String file) {
		return file.substring(file.lastIndexOf("/") + 1);
	}
}
