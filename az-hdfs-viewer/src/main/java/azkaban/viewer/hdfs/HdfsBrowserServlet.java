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
package azkaban.viewer.hdfs;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.server.session.Session;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.webapp.servlet.Page;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HdfsBrowserServlet extends LoginAbstractAzkabanServlet {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsBrowserServlet.class);
  private static final long serialVersionUID = 1L;
  private static final String PROXY_USER_SESSION_KEY = "hdfs.browser.proxy.user";
  private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = "hadoop.security.manager.class";
  private static final String HDFSVIEWER_ACCESS_DENIED_MESSAGE = "viewer.access_denied_message";
  private static final int DEFAULT_FILE_MAX_LINES = 1000;

  private int fileMaxLines;
  private int defaultStartLine;
  private int defaultEndLine;
  private ArrayList<HdfsFileViewer> viewers = new ArrayList<HdfsFileViewer>();

  private HdfsFileViewer defaultViewer;

  private Props props;
  private boolean shouldProxy;
  private boolean allowGroupProxy;

  private String viewerName;
  private String viewerPath;

  private HadoopSecurityManager hadoopSecurityManager;

  public HdfsBrowserServlet(Props props) {
    this.props = props;
    viewerName = props.getString("viewer.name");
    viewerPath = props.getString("viewer.path");
    fileMaxLines = props.getInt("file.max.lines", DEFAULT_FILE_MAX_LINES);
    defaultStartLine = 1;
    defaultEndLine = fileMaxLines;
  }

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);

    shouldProxy = props.getBoolean("azkaban.should.proxy", false);
    allowGroupProxy = props.getBoolean("allow.group.proxy", false);
    LOG.info("Hdfs browser should proxy: " + shouldProxy);

    props.put("fs.hdfs.impl.disable.cache", "true");

    try {
      hadoopSecurityManager = loadHadoopSecurityManager(props);
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to get hadoop security manager!"
          + e.getCause());
    }

    defaultViewer = new TextFileViewer();

    viewers.add(new HtmlFileViewer());
    viewers.add(new ORCFileViewer());
    viewers.add(new AvroFileViewer());
    viewers.add(new ParquetFileViewer());
//    viewers.add(new JsonSequenceFileViewer());
    viewers.add(new ImageFileViewer());
    viewers.add(new BsonFileViewer());

    viewers.add(defaultViewer);

    LOG.info("HDFS Browser initiated");
  }

  private HadoopSecurityManager loadHadoopSecurityManager(Props props) throws RuntimeException {

    Class<?> hadoopSecurityManagerClass =
        props.getClass(HADOOP_SECURITY_MANAGER_CLASS_PARAM, true,
            HdfsBrowserServlet.class.getClassLoader());
    LOG.info("Initializing hadoop security manager "
        + hadoopSecurityManagerClass.getName());
    HadoopSecurityManager hadoopSecurityManager = null;

    try {
      Method getInstanceMethod =
          hadoopSecurityManagerClass.getMethod("getInstance", Props.class);
      hadoopSecurityManager =
          (HadoopSecurityManager) getInstanceMethod.invoke(
              hadoopSecurityManagerClass, props);
    } catch (InvocationTargetException e) {
      LOG.error("Could not instantiate Hadoop Security Manager "
          + hadoopSecurityManagerClass.getName() + e.getCause());
      throw new RuntimeException(e.getCause());
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e.getCause());
    }

    return hadoopSecurityManager;
  }

  private FileSystem getFileSystem(String username)
      throws HadoopSecurityManagerException {
    return hadoopSecurityManager.getFSAsUser(username);
  }

  private void errorPage(String user, HttpServletRequest req,
      HttpServletResponse resp, Session session, String error) {
    Page page =
        newPage(req, resp, session,
            "azkaban/viewer/hdfs/velocity/hdfs-browser.vm");
    page.add("error_message", "Error: " + error);
    page.add("user", user);
    page.add("allowproxy", allowGroupProxy);
    page.add("no_fs", "true");
    page.add("viewerName", viewerName);
    page.render();
  }

  private void errorAjax(HttpServletResponse resp, Map<String, Object> ret,
      String error) throws IOException {
    ret.put("error", error);
    this.writeJSON(resp, ret);
  }

  private String getUsername(HttpServletRequest req, Session session)
      throws ServletException {
    User user = session.getUser();
    String username = user.getUserId();
    if (allowGroupProxy) {
      String proxyName =
          (String) session.getSessionData(PROXY_USER_SESSION_KEY);
      if (proxyName != null) {
        username = proxyName;
      }
    }
    return username;
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
    String username = getUsername(req, session);
    boolean ajax = hasParam(req, "ajax");
    try {
      if (ajax) {
        handleAjaxAction(username, req, resp, session);
      } else {
        handleFsDisplay(username, req, resp, session);
      }
    } catch (Exception e) {
      throw new IllegalStateException("Error processing request: "
          + e.getMessage(), e);
    }
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws ServletException, IOException {
    User user = session.getUser();
    if (!hasParam(req, "action")) {
      return;
    }

    HashMap<String, String> results = new HashMap<String, String>();
    String action = getParam(req, "action");
    if (action.equals("changeProxyUser")) {
      if (hasParam(req, "proxyname")) {
        String newProxyname = getParam(req, "proxyname");
        if (user.getUserId().equals(newProxyname)
            || user.isInGroup(newProxyname)
            || user.getRoles().contains("admin")) {
          session.setSessionData(PROXY_USER_SESSION_KEY, newProxyname);
        } else {
          results.put("error", "User '" + user.getUserId()
              + "' cannot proxy as '" + newProxyname + "'");
        }
      }
    } else {
      results.put("error", "action param is not set");
    }

    this.writeJSON(resp, results);
  }

  private Path getPath(HttpServletRequest req) {
    String prefix = req.getContextPath() + req.getServletPath();
    String fsPath = req.getRequestURI().substring(prefix.length());
    if (fsPath.length() == 0) {
      fsPath = "/";
    }
    return new Path(fsPath);
  }

  private void getPathSegments(Path path, List<Path> paths,
      List<String> segments) {
    Path curr = path;
    while (curr.getParent() != null) {
      paths.add(curr);
      segments.add(curr.getName());
      curr = curr.getParent();
    }
    Collections.reverse(paths);
    Collections.reverse(segments);
  }

  private String getHomeDir(FileSystem fs) {
    String homeDirString = fs.getHomeDirectory().toString();
    if (homeDirString.startsWith("file:")) {
      return homeDirString.substring("file:".length());
    }
    return homeDirString.substring(fs.getUri().toString().length());
  }

  private void handleFsDisplay(String user, HttpServletRequest req,
      HttpServletResponse resp, Session session) throws IOException,
      ServletException, IllegalArgumentException, IllegalStateException {
    FileSystem fs = null;
    try {
      fs = getFileSystem(user);
    } catch (HadoopSecurityManagerException e) {
      errorPage(user, req, resp, session, "Cannot get FileSystem.");
      return;
    }

    Path path = getPath(req);
    if (LOG.isDebugEnabled()) {
      LOG.debug("path: '" + path.toString() + "'");
    }

    try {
      if (!fs.exists(path)) {
        errorPage(user, req, resp, session, path.toUri().getPath()
            + " does not exist.");
        fs.close();
        return;
      }
    } catch (IOException ioe) {
      LOG.error("Got exception while checking for existence of path '"
          + path + "'", ioe);
      errorPage(user, req, resp, session, path.toUri().getPath()
          + " Encountered error while trying to detect if path '" + path
          + "' exists. Reason: " + ioe.getMessage());
      fs.close();
      return;
    }

    if (fs.isFile(path)) {
      displayFilePage(fs, user, req, resp, session, path);
    } else if (fs.getFileStatus(path).isDir()) {
      displayDirPage(fs, user, req, resp, session, path);
    } else {
      errorPage(user, req, resp, session,
          "It exists, it is not a file, and it is not a directory, what "
              + "is it precious?");
    }
    fs.close();
  }

  private void displayDirPage(FileSystem fs, String user,
      HttpServletRequest req, HttpServletResponse resp, Session session,
      Path path) throws IOException {

    Page page =
        newPage(req, resp, session,
            "azkaban/viewer/hdfs/velocity/hdfs-browser.vm");
    page.add("allowproxy", allowGroupProxy);
    page.add("viewerPath", viewerPath);
    page.add("viewerName", viewerName);

    List<Path> paths = new ArrayList<Path>();
    List<String> segments = new ArrayList<String>();
    getPathSegments(path, paths, segments);
    page.add("paths", paths);
    page.add("segments", segments);
    page.add("user", user);
    page.add("homedir", getHomeDir(fs));

    try {
      FileStatus[] subdirs = fs.listStatus(path);
      page.add("subdirs", subdirs);
      long size = 0;
      for (int i = 0; i < subdirs.length; ++i) {
        if (subdirs[i].isDir()) {
          continue;
        }
        size += subdirs[i].getLen();
      }
      page.add("dirsize", size);
    } catch (AccessControlException e) {
      String error_message = props.getString(HDFSVIEWER_ACCESS_DENIED_MESSAGE);
      page.add("error_message", "Permission denied: " + error_message);
      page.add("no_fs", "true");
    } catch (IOException e) {
      page.add("error_message", "Error: " + e.getMessage());
    }
    page.render();
  }

  private void displayFilePage(FileSystem fs, String user,
      HttpServletRequest req, HttpServletResponse resp, Session session,
      Path path) {

    Page page =
        newPage(req, resp, session, "azkaban/viewer/hdfs/velocity/hdfs-file.vm");

    List<Path> paths = new ArrayList<Path>();
    List<String> segments = new ArrayList<String>();
    getPathSegments(path, paths, segments);

    page.add("allowproxy", allowGroupProxy);
    page.add("viewerPath", viewerPath);
    page.add("viewerName", viewerName);

    page.add("paths", paths);
    page.add("segments", segments);
    page.add("user", user);
    page.add("path", path.toString());
    page.add("homedir", getHomeDir(fs));

    try {
      boolean hasSchema = false;
      int viewerId = -1;
      for (int i = 0; i < viewers.size(); ++i) {
        HdfsFileViewer viewer = viewers.get(i);
        Set<Capability> capabilities = EnumSet.noneOf(Capability.class);
        capabilities = viewer.getCapabilities(fs, path);
        if (capabilities.contains(Capability.READ)) {
          if (capabilities.contains(Capability.SCHEMA)) {
            hasSchema = true;
          }
          viewerId = i;
          break;
        }
      }
      page.add("contentType", viewers.get(viewerId).getContentType().name());
      page.add("viewerId", viewerId);
      page.add("hasSchema", hasSchema);

      FileStatus status = fs.getFileStatus(path);
      page.add("status", status);

    } catch (Exception ex) {
      page.add("no_fs", "true");
      page.add("error_message", "Error: " + ex.getMessage());
    }
    page.render();
  }

  private void handleAjaxAction(String username, HttpServletRequest request,
      HttpServletResponse response, Session session) throws ServletException,
      IOException {
    Map<String, Object> ret = new HashMap<String, Object>();
    FileSystem fs = null;
    try {
      try {
        fs = getFileSystem(username);
      } catch (HadoopSecurityManagerException e) {
        errorAjax(response, ret, "Cannot get FileSystem.");
        return;
      }

      String ajaxName = getParam(request, "ajax");
      Path path = null;
      if (!hasParam(request, "path")) {
        errorAjax(response, ret, "Missing parameter 'path'.");
        return;
      }

      path = new Path(getParam(request, "path"));
      if (!fs.exists(path)) {
        errorAjax(response, ret, path.toUri().getPath() + " does not exist.");
        return;
      }

      if (ajaxName.equals("fetchschema")) {
        handleAjaxFetchSchema(fs, request, ret, session, path);
      } else if (ajaxName.equals("fetchfile")) {
        // Note: fetchFile writes directly to the output stream. Thus, we need
        // to make sure we do not write to the output stream once this call
        // returns.
        ret = null;
        handleAjaxFetchFile(fs, request, response, session, path);
      } else {
        ret.put("error", "Unknown AJAX action " + ajaxName);
      }

      if (ret != null) {
        this.writeJSON(response, ret);
      }
    } finally {
      fs.close();
    }
  }

  private void handleAjaxFetchSchema(FileSystem fs, HttpServletRequest req,
      Map<String, Object> ret, Session session, Path path) throws IOException,
      ServletException {
    HdfsFileViewer fileViewer = null;
    try {
      if (hasParam(req, "viewerId")) {
        fileViewer = viewers.get(getIntParam(req, "viewerId"));
        if (!fileViewer.getCapabilities(fs, path).contains(Capability.SCHEMA)) {
          fileViewer = null;
        }
      } else {
        for (HdfsFileViewer viewer : viewers) {
          if (viewer.getCapabilities(fs, path).contains(Capability.SCHEMA)) {
            fileViewer = viewer;
          }
        }
      }
    } catch (AccessControlException e) {
      ret.put("error", "Permission denied.");
    }

    if (fileViewer == null) {
      ret.put("error", "No viewers can display schema.");
      return;
    }
    ret.put("schema", fileViewer.getSchema(fs, path));
  }

  private void handleAjaxFetchFile(FileSystem fs, HttpServletRequest req,
      HttpServletResponse resp, Session session, Path path) throws IOException,
      ServletException {
    int startLine = getIntParam(req, "startLine", defaultStartLine);
    int endLine = getIntParam(req, "endLine", defaultEndLine);
    OutputStream output = resp.getOutputStream();

    if (endLine < startLine) {
      output.write(("Invalid range: endLine < startLine.").getBytes("UTF-8"));
      return;
    }

    if (endLine - startLine > fileMaxLines) {
      output.write(("Invalid range: range exceeds max number of lines.")
          .getBytes("UTF-8"));
      return;
    }

    // Use registered viewers to show the file content
    HdfsFileViewer fileViewer = null;
    try {
      if (hasParam(req, "viewerId")) {
        fileViewer = viewers.get(getIntParam(req, "viewerId"));
        if (!fileViewer.getCapabilities(fs, path).contains(Capability.READ)) {
          fileViewer = null;
        }
      } else {
        for (HdfsFileViewer viewer : viewers) {
          if (viewer.getCapabilities(fs, path).contains(Capability.READ)) {
            fileViewer = viewer;
            break;
          }
        }
      }
      // use default text viewer
      if (fileViewer == null) {
        if (defaultViewer.getCapabilities(fs, path).contains(Capability.READ)) {
          fileViewer = defaultViewer;
        } else {
          output.write(("No viewer available for file.").getBytes("UTF-8"));
          return;
        }
      }
    } catch (AccessControlException e) {
      output.write(("Permission denied.").getBytes("UTF-8"));
    }

    fileViewer.displayFile(fs, path, output, startLine, endLine);
  }
}
