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
			logger.error("User " + user.getUserId() + " has no permission to write to project " + project.getName());
			throw new ProjectManagerException("User '" + user + "' doesn't have permissions to deploy to " + project.getName());
		}

		// Deploy stuff here. Move the code to a more formal area later.
		logger.info("Downloading file from " + packageUrl);
		URL url = null;
		InputStream in = null;
		try {
			url = new URL(packageUrl);
			in = url.openStream();
		} catch (MalformedURLException e) {
			logger.error("Url " + packageUrl + " is malformed.", e);
			throw new ProjectManagerException("Url " + packageUrl + " is malformed.", e);
		} catch (IOException e) {
			logger.error("Error opening input stream.", e);
			throw new ProjectManagerException("Error opening input stream. Couldn't download file.", e);
		}
		
		String filename = getFileName(url.getFile());

		BufferedInputStream buff = new BufferedInputStream(in);
		File tempDir = Utils.createTempDir();
		OutputStream out = null;
		
		try {
			logger.error("Downloading " + filename);
			File archiveFile = new File(tempDir, filename);
			out = new BufferedOutputStream(new FileOutputStream(archiveFile));
			IOUtils.copy(buff, out);
			out.close();
			
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
			if (buff != null) {
				buff.close();
			}
			if (out != null) {
				out.close();
			}
		}
		
		return Integer.toString(project.getVersion());
	}

	private String getFileName(String file) {
		return file.substring(file.lastIndexOf("/") + 1);
	}
}
