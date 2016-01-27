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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.mortbay.io.WriterOutputStream;

import azkaban.project.Project;
import azkaban.server.AzkabanServer;
import azkaban.server.session.Session;
import azkaban.user.User;
import azkaban.webapp.AzkabanWebServer;

public class FileEditorServlet extends LoginAbstractAzkabanServlet {

	private static final long serialVersionUID = 1L;
	private static final String projectsBaseDirectory = AzkabanServer.getAzkabanProperties().get("azkaban.project.dir");
	private static final String externalDirectory = AzkabanServer.getAzkabanProperties().get("external.dir");
	private static final Logger logger = Logger.getLogger(FileEditorServlet.class.getName());

	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
			throws ServletException, IOException {
		if (hasParam(req, "resource")) {
			String resourcePath = getParam(req, "resource");
			logger.info("Fetching : " + resourcePath);
			streamProjectResource(resourcePath, resp, session.getUser());
		} else if (hasParam(req, "externalresource")) {
			String resourcePath = getParam(req, "externalresource");
			logger.info("Fetching : " + resourcePath);
			streamExternalResource(resourcePath, resp);
		} else {
			Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/fileeditor.vm");
			page.render();
		}
	}

	private void streamExternalResource(String resourcePath, HttpServletResponse response)
			throws IOException, ServletException {
		File resource = new File(externalDirectory + resourcePath);
		if (resource.exists()) {
			response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");// HTTP
																						// 1.1.
			response.setHeader("Pragma", "no-cache"); // HTTP 1.0.
			response.setDateHeader("Expires", 0); // Proxies
			response.setStatus(HttpServletResponse.SC_OK);
			if (resource.isDirectory()) {
				OutputStream out = fetchOutputStream(response);
				String directoryContent = getExternalDirectoryContent(resourcePath);
				logger.debug("DirectoryContent " + directoryContent);
				response.setContentType("text/html");
				response.setContentLength(directoryContent.length());
				out.write(directoryContent.toString().getBytes());
			} else {
				streamFileContent(resourcePath, externalDirectory, response);
			}
		}
	}

	private String getExternalDirectoryContent(String resourcePath) {
		StringBuffer directoryContent = new StringBuffer();

		File queryFolder = new File(externalDirectory + resourcePath);

		String[] queryFolderContent = queryFolder.list();

		directoryContent.append("<table border=0>");

		// Insert a link to the parent folder only if the resource is not the
		// webserver's context root
		if (!"/".equals(resourcePath) && resourcePath != null && resourcePath.length() > 0) {
			File parent = queryFolder.getParentFile();
			directoryContent.append("<tr><td><a href=\"#\" onclick=\"browseConfigDirectory('"
					+ parent.getAbsolutePath().substring(externalDirectory.length())
					+ "/')\">[Parent Folder]</td></tr>");
		}

		for (String resourceName : queryFolderContent) {
			File resourceFile = new File(externalDirectory + resourcePath + resourceName);
			if (resourceFile.isDirectory()) {
				directoryContent.append("<tr><td>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"#\" onclick=\"browseConfigDirectory('"
						+ resourcePath + resourceName + "/')\">" + resourceName + "/</td></tr>");
			} else {
				directoryContent.append("<tr><td>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"#\" onclick=\"fetchConfigFileContent('"
						+ resourcePath + resourceName + "')\">" + resourceName + "</td></tr>");
			}
		}
		directoryContent.append("</table><br>");
		return directoryContent.toString();
	}

	private OutputStream fetchOutputStream(HttpServletResponse response) throws IOException {
		OutputStream out = null;
		try {
			out = response.getOutputStream();
		} catch (IllegalStateException e) {
			out = new WriterOutputStream(response.getWriter());
		}
		return out;
	}

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
			throws ServletException, IOException {
		if (hasParam(req, "resource")) {
			String resourcePath = getParam(req, "resource");
			String content = req.getParameter("content");
			logger.info("Saving to : " + resourcePath);
			saveResource(resourcePath, projectsBaseDirectory,content);
		} else if (hasParam(req, "externalresource")) {
			String resourcePath = getParam(req, "externalresource");
			String content = req.getParameter("content");
			logger.info("Saving to : " + resourcePath);
			saveResource(resourcePath,externalDirectory, content);
		}
	}

	private void streamProjectResource(String resourcePath, HttpServletResponse response, User user)
			throws IOException, ServletException {

		File resource = new File(projectsBaseDirectory + resourcePath);

		if (resource.exists()) {
			response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");// HTTP
																						// 1.1.
			response.setHeader("Pragma", "no-cache"); // HTTP 1.0.
			response.setDateHeader("Expires", 0); // Proxies
			response.setStatus(HttpServletResponse.SC_OK);
			if (resource.isDirectory()) {
				OutputStream out = fetchOutputStream(response);
				String directoryContent = getProjectDirectoryContent(resourcePath, user);
				logger.debug("DirectoryContent " + directoryContent);
				response.setContentType("text/html");
				response.setContentLength(directoryContent.length());
				out.write(directoryContent.toString().getBytes());
			} else {
				streamFileContent(resourcePath, projectsBaseDirectory, response);
			}
		}
	}

	private String getProjectDirectoryContent(String resourcePath, User user) {
		StringBuffer directoryContent = new StringBuffer();

		File baseFolder = new File(projectsBaseDirectory);
		File queryFolder = new File(projectsBaseDirectory + resourcePath);

		String[] queryFolderContent = queryFolder.list();

		directoryContent.append("<table border=0>");

		// Insert a link to the parent folder only if the resource is not the
		// webserver's context root
		if (!"/".equals(resourcePath) && resourcePath != null && resourcePath.length() > 0) {
			File parent = queryFolder.getParentFile();
			directoryContent.append("<tr><td><a href=\"#\" onclick=\"browseProjectDirectory('"
					+ parent.getAbsolutePath().substring(projectsBaseDirectory.length())
					+ "/')\">[Parent Folder]</td></tr>");
		}

		// Fetch all projects accessible to the current session user
		List<Project> projects = ((AzkabanWebServer) getApplication()).getProjectManager().getUserProjects(user);
		List<String> projectNames = new ArrayList<String>();
		for (Project project : projects) {
			projectNames.add(project.getName());
		}

		for (String resourceName : queryFolderContent) {
			File resourceFile = new File(projectsBaseDirectory + resourcePath + resourceName);
			// If the folder is at the first level under context root, then
			// it should be an azkaban project
			if (resourceFile.getParent().equals(baseFolder.getPath())) {
				// The first level should contain only azkaban project
				// folders
				if (resourceFile.isDirectory()) {
					// The project name of this folder is in a special text
					// file named "projectname", one level under this folder
					String displayName = getDisplayName(
							resourceFile.getAbsolutePath() + File.separator + "projectname");
					// The access to this folder is filtered out
					// based on the currently session user
					if (projectNames.contains(displayName)) {
						directoryContent
								.append("<tr><td>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"#\" onclick=\"browseProjectDirectory('"
										+ resourcePath + resourceName + "/')\">"
										+ (displayName == null ? resourceName : displayName) + "/</td></tr>");
					}
				}
			}
			// The folder is NOT at the first level under context root, then
			// hence is a deeper sub folder under one of the azkaban project
			else {
				// All folders at this deeper level should be accessible
				if (resourceFile.isDirectory()) {
					directoryContent.append("<tr><td>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"#\" onclick=\"browseProjectDirectory('"
							+ resourcePath + resourceName + "/')\">" + resourceName + "/</td></tr>");
				}
				// All files at this deeper level should be accessible,
				// except the special ones namd "projectname"
				else {
					if (!"projectname".equals(resourceName)) {
						directoryContent
								.append("<tr><td>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"#\" onclick=\"fetchProjectFileContent('"
										+ resourcePath + resourceName + "')\">" + resourceName + "</td></tr>");
					}
				}
			}
		}
		directoryContent.append("</table><br>");
		return directoryContent.toString();
	}

	private String getDisplayName(String projectNameFile) {
		String displayName = null;
		Scanner in = null;
		try {
			in = new Scanner(new File(projectNameFile));
			displayName = in.nextLine();
		} catch (FileNotFoundException e) {
			// The default displayName is used in case of an
			// Exception
		} finally {
			if (in != null) {
				in.close();
			}
		}
		return displayName;
	}

	private void streamFileContent(String resourcePath, String baseDirectory, HttpServletResponse response)
			throws IOException {
		response.setContentType("text/plain");
		response.setStatus(HttpServletResponse.SC_OK);

		OutputStream out = fetchOutputStream(response);

		int len, totalLength = 0;
		FileInputStream in = null;
		try {
			in = new FileInputStream(baseDirectory + resourcePath);
			int bufferSize = 2 * 8192;
			byte buffer[] = new byte[bufferSize];

			while (true) {
				len = in.read(buffer, 0, bufferSize);
				totalLength += len;
				if (len < 0)
					break;
				out.write(buffer, 0, len);
			}
		} finally {
			if (in != null) {
				in.close();
			}
		}
		response.setContentLength(totalLength + 1);
	}

	private void saveResource(String resourcePath,String baseDirectory, String content) throws IOException, ServletException {
		OutputStream out = null;
		try {
			File resource = new File(baseDirectory + resourcePath);
			if (resource.exists() && resource.isFile()) {
				out = new FileOutputStream(resource);
				out.write((content + "").getBytes());
			}
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}
}
