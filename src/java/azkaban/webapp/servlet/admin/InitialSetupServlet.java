/*
 * Copyright 2012 LinkedIn, Inc
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

package azkaban.webapp.servlet.admin;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import azkaban.utils.DataSourceUtils;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import azkaban.webapp.AzkabanAdminServer;
import azkaban.webapp.servlet.AbstractAzkabanServlet;
import azkaban.webapp.servlet.MultipartParser;
import azkaban.webapp.servlet.Page;

/**
 * The main page
 */
public class InitialSetupServlet extends AbstractAzkabanServlet {

	private static final long serialVersionUID = -1;
	private static final int DEFAULT_UPLOAD_DISK_SPOOL_SIZE = 20 * 1024 * 1024;
	private static final String DB_DIRECTORY = "db";
	private static final String LDAP_DIRECTORY = "auth";

    private static Logger logger = Logger.getLogger(InitialSetupServlet.class);

    private File mysqlConnectorJar;
    private File propsFile;
	private String pluginLibDirectory;
	private MultipartParser multipartParser;
	private Props props;
	
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		AzkabanAdminServer server = (AzkabanAdminServer)getApplication();
		pluginLibDirectory = server.getPluginLibDirectory();
		props = server.getServerProps();
		multipartParser = new MultipartParser(DEFAULT_UPLOAD_DISK_SPOOL_SIZE);
		mysqlConnectorJar = getConnectorJar();
		propsFile = server.getPropFile();
	}
	
	private File getConnectorJar() {
		File pluginLibs = new File(pluginLibDirectory);
		File dbPluginDir = new File(pluginLibs, DB_DIRECTORY);
		if (dbPluginDir.exists()) {
			File[] listFiles = dbPluginDir.listFiles(new PrefixSuffixFilter("mysql-connector", ".jar"));
			if (listFiles.length > 0) {
				return listFiles[0];
			}
		}
		
		return null;
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		if (hasParam(req, "dbsetup")) {
			Page page = newPage(req, resp, "azkaban/webapp/servlet/admin/velocity/setup-db.vm");
			
			String host = props.getString("mysql.host", "localhost");
			int port = props.getInt("mysql.port", 3306);
			String database = props.getString("mysql.database", "");
			String username = props.getString("mysql.username", "");
			String password = props.getString("mysql.password", "");
			
			page.add("host", host);
			page.add("port", port);
			page.add("database", database);
			page.add("username", username);
			page.add("password", password);
			
			if (mysqlConnectorJar != null) {
				page.add("installedJar", mysqlConnectorJar.getName());
				
				if (props.containsKey("mysql.host")) {
					URL url = mysqlConnectorJar.toURI().toURL(); 
					URL[] urls = new URL[] {url};
					ClassLoader mySQLLoader = new URLClassLoader(urls, InitialSetupServlet.class.getClassLoader());
					
					try {
						Utils.invokeStaticMethod(mySQLLoader, DataSourceUtils.class.getName(), "testMySQLConnection", host, port, database, username, password, 1);
						page.add("message", "Able to connect to DB.");
						page.add("verified", true);
					} catch (InvocationTargetException e) {
						page.add("message", "");
						page.add("verified", false);
					}
					catch (Exception e) {
						page.add("message", "");
						page.add("verified", false);
					}
				}
			}

			page.render();
		}
		else if (hasParam(req, "usersetup")) {
			Page page = newPage(req, resp, "azkaban/webapp/servlet/admin/velocity/setup-usermanager.vm");
			page.render();
		}
		else {
			Page page = newPage(req, resp, "azkaban/webapp/servlet/admin/velocity/setup-initial.vm");
			page.render();
		}
	}
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		HashMap<String, Object> jsonResponse = new HashMap<String,Object>();
		
		if (ServletFileUpload.isMultipartContent(req)) {
			logger.info("Post is multipart");
			Map<String, Object> params = multipartParser.parseMultipart(req);
			if (params.containsKey("action")) {
				String action = (String)params.get("action");
				if (action.equals("upload")) {
					handleUpload(req, resp, params, jsonResponse);
				}
			}
		}
		else if (hasParam(req, "ajax")){
			try {
				String action = getParam(req, "ajax");
				if (action.equals("saveDbConnection")) {
					handleSaveDBConnection(req, resp, jsonResponse);
				}
			} 
			catch (InvocationTargetException e) {
				jsonResponse.put("error", e.getTargetException().getMessage());
			}
			catch (Exception e) {
				jsonResponse.put("error", e.getMessage());
			}
		}

		this.writeJSON(resp, jsonResponse);
	}

	private void handleSaveDBConnection(HttpServletRequest req, HttpServletResponse resp, Map<String, Object> jsonResponse) throws Exception {
		if (mysqlConnectorJar == null) {
			jsonResponse.put("error", "No connector jar has been installed");
		}
		else {
			String host = getParam(req, "host");
			int port = getIntParam(req, "port");
			String database = getParam(req, "database");
			String username = getParam(req, "username");
			String password = getParam(req, "password");
			
			URL url = mysqlConnectorJar.toURI().toURL(); 
			URL[] urls = new URL[] {url};
			ClassLoader mySQLLoader = new URLClassLoader(urls, InitialSetupServlet.class.getClassLoader());
			
			Utils.invokeStaticMethod(mySQLLoader, DataSourceUtils.class.getName(), "testMySQLConnection", host, port, database, username, password, 1);

			props.put("mysql.host", host);
			props.put("mysql.port", port);
			props.put("mysql.database", database);
			props.put("mysql.username", username);
			props.put("mysql.password", password);
			
			logger.info("Print file " + propsFile.getPath());
			props.storeLocal(propsFile);
			
			jsonResponse.put("success", "Able to connect to DB.");
		}
	}
	
	private void handleUpload(HttpServletRequest req, HttpServletResponse resp, Map<String, Object> multipart, Map<String, Object> jsonResponse) throws ServletException, IOException {
		FileItem item = (FileItem) multipart.get("file");
		String name = item.getName();
		
		final String contentType = item.getContentType();

		File tempDir = Utils.createTempDir();
		OutputStream out = null;
		try {
			logger.info("Uploading file " + name);
			File archiveFile = new File(tempDir, name);
			out = new BufferedOutputStream(new FileOutputStream(archiveFile));
			IOUtils.copy(item.getInputStream(), out);
			out.close();
			
			File pluginLibs = new File(pluginLibDirectory);
			File dbPluginDir = new File(pluginLibs, DB_DIRECTORY);
			if (dbPluginDir.exists()) {
				
			}
			if (!dbPluginDir.exists()) {
				dbPluginDir.mkdirs();
			}
			else {
				FileUtils.cleanDirectory(dbPluginDir);
			}
			
			File newName = new File(dbPluginDir, archiveFile.getName());
			archiveFile.renameTo(newName);
			mysqlConnectorJar = newName;

			jsonResponse.put("jarname", archiveFile.getName());
		} catch (Exception e) {
			jsonResponse.put("error", "Failed:" + e.getMessage());
		}
		finally {
			if (tempDir.exists()) {
				FileUtils.deleteDirectory(tempDir);
			}
			if (out != null) {
				out.close();
			}
		}
	}

	private static class PrefixSuffixFilter implements FileFilter {
		private String prefix;
		private String suffix;
		
		public PrefixSuffixFilter(String prefix, String suffix) {
			this.prefix = prefix;
			this.suffix = suffix;
		}

		@Override
		public boolean accept(File pathname) {
			String name = pathname.getName();

			if (!pathname.isFile() || pathname.isHidden()) {
				return false;
			}
			
			if (name.length() < prefix.length() || !name.startsWith(prefix)) {
				return false;
			}
			
			if (name.length() < suffix.length() || !name.endsWith(suffix)) {
				return false;
			}
			
			return true;
		}
	}
}
