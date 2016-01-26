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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.mortbay.io.WriterOutputStream;

import azkaban.server.AzkabanServer;
import azkaban.server.session.Session;

public class FileEditorServlet extends LoginAbstractAzkabanServlet {

	private static final long serialVersionUID = 1L;
	private static final String basePath = AzkabanServer.getAzkabanProperties().get("azkaban.project.dir");

	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
			throws ServletException, IOException {
		if (hasParam(req, "resource")) {
			String resourcePath = getParam(req, "resource");
			streamResource(resourcePath, resp);
		} else {
			Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/fileeditor.vm");
			page.render();
		}
	}

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
			throws ServletException, IOException {
		if (hasParam(req, "resource")) {
			String resourcePath = getParam(req, "resource");
			String content = req.getParameter("content");
			saveResource(resourcePath, content);
		}
	}

	private void streamResource(String resourcePath, HttpServletResponse response)
			throws IOException, ServletException {

		File resource = new File(basePath + resourcePath);
		OutputStream out = null;
		try {
			out = response.getOutputStream();
		} catch (IllegalStateException e) {
			out = new WriterOutputStream(response.getWriter());
		}

		if (resource.exists()) {
			if (resource.isDirectory()) {
				String directoryContent = getDirectoryContent(resourcePath);
				response.setContentType("text/html");
				response.setContentLength(directoryContent.length());
				response.setStatus(HttpServletResponse.SC_OK);
				out.write(directoryContent.toString().getBytes());
			} else {
				response.setContentType("text/plain");
				response.setStatus(HttpServletResponse.SC_OK);
				int length = streamFileContent(resourcePath, out);
				response.setContentLength(length);
			}
		}
	}

	private String getDirectoryContent(String resourcePath) {
		StringBuffer directoryContent = new StringBuffer();
		Collection<File> files = FileUtils.listFilesAndDirs(new File(basePath + resourcePath), TrueFileFilter.INSTANCE,
				TrueFileFilter.INSTANCE);
		directoryContent.append("<table border=0>");

		if (!"/".equals(resourcePath) && resourcePath != null && resourcePath.length() > 0) {
			File parent = new File(basePath + resourcePath).getParentFile();
			directoryContent.append("<tr><td><a href=\"#\" onclick=\"browseDirectory('"
					+ parent.getAbsolutePath().substring(basePath.length()) + "/')\">[Parent Folder]</td></tr>");
		}

		for (File file : files) {
			if (file.getParent().equals(new File(basePath + resourcePath).getPath())) {
				if (file.isDirectory()) {
					directoryContent.append("<tr><td>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"#\" onclick=\"browseDirectory('"
							+ resourcePath + file.getName() + "/')\">" + file.getName() + "/</td></tr>");
				} else {
					directoryContent.append("<tr><td>&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"#\" onclick=\"fetchFileContent('"
							+ resourcePath + file.getName() + "')\">" + file.getName() + "</td></tr>");
				}
			}
		}
		directoryContent.append("</table><br>");
		return directoryContent.toString();
	}

	private int streamFileContent(String resourcePath, OutputStream out) throws IOException {
		int len, totalLength = 0;
		FileInputStream in = null;
		try {
			in = new FileInputStream(basePath + resourcePath);
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
		return totalLength + 1;
	}

	private void saveResource(String resourcePath, String content) throws IOException, ServletException {
		OutputStream out = null;
		try {
			File resource = new File(basePath + resourcePath);
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
