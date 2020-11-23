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
import azkaban.server.session.Session;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.webapp.servlet.Page;
import java.io.IOException;
import java.io.OutputStream;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.log4j.Logger;


public class HdfsBrowserServlet extends LoginAbstractAzkabanServlet {

  private static final long serialVersionUID = 1L;
  private static final String PROXY_USER_SESSION_KEY = "hdfs.browser.proxy.user";
  private static final String HADOOP_SECURITY_MANAGER_CLASS_PARAM = "hadoop.security.manager.class";
  private static final String HDFSVIEWER_ACCESS_DENIED_MESSAGE = "viewer.access_denied_message";

  private static final int DEFAULT_FILE_MAX_LINES = 1000;
  private static final Logger logger = Logger.getLogger(HdfsBrowserServlet.class);
  private final int fileMaxLines;
  private final int defaultStartLine;
  private final int defaultEndLine;
  private final ArrayList<HdfsFileViewer> viewers = new ArrayList<>();

  private HdfsFileViewer defaultViewer;

  private final Props props;
  private boolean shouldProxy;
  private boolean allowGroupProxy;

  private final String viewerName;
  private final String viewerPath;

  private HadoopSecurityManager hadoopSecurityManager;

  public HdfsBrowserServlet(final Props props) {
    super(new ArrayList<>());
    this.props = props;
    this.viewerName = props.getString("viewer.name");
    this.viewerPath = props.getString("viewer.path");
    this.fileMaxLines = props.getInt("file.max.lines", DEFAULT_FILE_MAX_LINES);
    this.defaultStartLine = 1;
    this.defaultEndLine = this.fileMaxLines;
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);

    this.shouldProxy = this.props.getBoolean("azkaban.should.proxy", false);
    this.allowGroupProxy = this.props.getBoolean("allow.group.proxy", false);
    logger.info("Hdfs browser should proxy: " + this.shouldProxy);

    this.props.put("fs.hdfs.impl.disable.cache", "true");

    try {
      this.hadoopSecurityManager = loadHadoopSecurityManager(this.props, logger);
    } catch (final RuntimeException e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to get hadoop security manager!"
          + e.getCause());
    }

    this.defaultViewer = new TextFileViewer();

    this.viewers.add(new HtmlFileViewer());
    this.viewers.add(new ORCFileViewer());
    this.viewers.add(new AvroFileViewer());
    this.viewers.add(new ParquetFileViewer());
//    viewers.add(new JsonSequenceFileViewer());
    this.viewers.add(new ImageFileViewer());
    this.viewers.add(new BsonFileViewer());

    this.viewers.add(this.defaultViewer);

    logger.info("HDFS Browser initiated");
  }

  private HadoopSecurityManager loadHadoopSecurityManager(final Props props,
      final Logger logger) throws RuntimeException {

    final Class<?> hadoopSecurityManagerClass =
        props.getClass(HADOOP_SECURITY_MANAGER_CLASS_PARAM, true,
            HdfsBrowserServlet.class.getClassLoader());
    logger.info("Initializing hadoop security manager "
        + hadoopSecurityManagerClass.getName());
    HadoopSecurityManager hadoopSecurityManager = null;

    try {
      hadoopSecurityManager = (HadoopSecurityManager) Utils.callConstructor(
          hadoopSecurityManagerClass, props);
    } catch (final Exception e) {
      logger.error("Could not instantiate Hadoop Security Manager "
          + hadoopSecurityManagerClass.getName() + e.getCause());
      throw new RuntimeException(e);
    }

    return hadoopSecurityManager;
  }

  private FileSystem getFileSystem(final String username)
      throws HadoopSecurityManagerException {
    return this.hadoopSecurityManager.getFSAsUser(username);
  }

  private void errorPage(final String user, final HttpServletRequest req,
      final HttpServletResponse resp, final Session session, final String error) {
    final Page page =
        newPage(req, resp, session,
            "azkaban/viewer/hdfs/velocity/hdfs-browser.vm");
    page.add("error_message", "Error: " + error);
    page.add("user", user);
    page.add("allowproxy", this.allowGroupProxy);
    page.add("no_fs", "true");
    page.add("viewerName", this.viewerName);
    page.render();
  }

  private void errorAjax(final HttpServletResponse resp, final Map<String, Object> ret,
      final String error) throws IOException {
    ret.put("error", error);
    this.writeJSON(resp, ret);
  }

  private String getUsername(final HttpServletRequest req, final Session session)
      throws ServletException {
    final User user = session.getUser();
    String username = user.getUserId();
    if (this.allowGroupProxy) {
      final String proxyName =
          (String) session.getSessionData(PROXY_USER_SESSION_KEY);
      if (proxyName != null) {
        username = proxyName;
      }
    }
    return username;
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    final String username = getUsername(req, session);
    final boolean ajax = hasParam(req, "ajax");
    try {
      if (ajax) {
        handleAjaxAction(username, req, resp, session);
      } else {
        handleFsDisplay(username, req, resp, session);
      }
    } catch (final Exception e) {
      throw new IllegalStateException("Error processing request: "
          + e.getMessage(), e);
    }
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    final User user = session.getUser();
    if (!hasParam(req, "action")) {
      return;
    }

    final HashMap<String, String> results = new HashMap<>();
    final String action = getParam(req, "action");
    if (action.equals("changeProxyUser")) {
      if (hasParam(req, "proxyname")) {
        final String newProxyname = getParam(req, "proxyname");
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

  private Path getPath(final HttpServletRequest req) {
    final String prefix = req.getContextPath() + req.getServletPath();
    String fsPath = req.getRequestURI().substring(prefix.length());
    if (fsPath.length() == 0) {
      fsPath = "/";
    }
    return new Path(fsPath);
  }

  private void getPathSegments(final Path path, final List<Path> paths,
      final List<String> segments) {
    Path curr = path;
    while (curr.getParent() != null) {
      paths.add(curr);
      segments.add(curr.getName());
      curr = curr.getParent();
    }
    Collections.reverse(paths);
    Collections.reverse(segments);
  }

  private String getHomeDir(final FileSystem fs) {
    final String homeDirString = fs.getHomeDirectory().toString();
    if (homeDirString.startsWith("file:")) {
      return homeDirString.substring("file:".length());
    }
    return homeDirString.substring(fs.getUri().toString().length());
  }

  private void handleFsDisplay(final String user, final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws IOException,
      ServletException, IllegalArgumentException, IllegalStateException {
    FileSystem fs = null;
    try {
      fs = getFileSystem(user);
    } catch (final HadoopSecurityManagerException e) {
      errorPage(user, req, resp, session, "Cannot get FileSystem.");
      return;
    }

    final Path path = getPath(req);
    if (logger.isDebugEnabled()) {
      logger.debug("path: '" + path.toString() + "'");
    }

    try {
      if (!fs.exists(path)) {
        errorPage(user, req, resp, session, path.toUri().getPath()
            + " does not exist.");
        fs.close();
        return;
      }
    } catch (final IOException ioe) {
      logger.error("Got exception while checking for existence of path '"
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

  private void displayDirPage(final FileSystem fs, final String user,
      final HttpServletRequest req, final HttpServletResponse resp, final Session session,
      final Path path) throws IOException {

    final Page page =
        newPage(req, resp, session,
            "azkaban/viewer/hdfs/velocity/hdfs-browser.vm");
    page.add("allowproxy", this.allowGroupProxy);
    page.add("viewerPath", this.viewerPath);
    page.add("viewerName", this.viewerName);

    final List<Path> paths = new ArrayList<>();
    final List<String> segments = new ArrayList<>();
    getPathSegments(path, paths, segments);
    page.add("paths", paths);
    page.add("segments", segments);
    page.add("user", user);
    page.add("homedir", getHomeDir(fs));

    try {
      final FileStatus[] subdirs = fs.listStatus(path);
      page.add("subdirs", subdirs);
      long size = 0;
      for (int i = 0; i < subdirs.length; ++i) {
        if (subdirs[i].isDir()) {
          continue;
        }
        size += subdirs[i].getLen();
      }
      page.add("dirsize", size);
    } catch (final AccessControlException e) {
      final String error_message = this.props.getString(HDFSVIEWER_ACCESS_DENIED_MESSAGE);
      page.add("error_message", "Permission denied: " + error_message);
      page.add("no_fs", "true");
    } catch (final IOException e) {
      page.add("error_message", "Error: " + e.getMessage());
    }
    page.render();
  }

  private void displayFilePage(final FileSystem fs, final String user,
      final HttpServletRequest req, final HttpServletResponse resp, final Session session,
      final Path path) {

    final Page page =
        newPage(req, resp, session, "azkaban/viewer/hdfs/velocity/hdfs-file.vm");

    final List<Path> paths = new ArrayList<>();
    final List<String> segments = new ArrayList<>();
    getPathSegments(path, paths, segments);

    page.add("allowproxy", this.allowGroupProxy);
    page.add("viewerPath", this.viewerPath);
    page.add("viewerName", this.viewerName);

    page.add("paths", paths);
    page.add("segments", segments);
    page.add("user", user);
    page.add("path", path.toString());
    page.add("homedir", getHomeDir(fs));

    try {
      boolean hasSchema = false;
      int viewerId = -1;
      for (int i = 0; i < this.viewers.size(); ++i) {
        final HdfsFileViewer viewer = this.viewers.get(i);
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
      page.add("contentType", this.viewers.get(viewerId).getContentType().name());
      page.add("viewerId", viewerId);
      page.add("hasSchema", hasSchema);

      final FileStatus status = fs.getFileStatus(path);
      page.add("status", status);

    } catch (final Exception ex) {
      page.add("no_fs", "true");
      page.add("error_message", "Error: " + ex.getMessage());
    }
    page.render();
  }

  private void handleAjaxAction(final String username, final HttpServletRequest request,
      final HttpServletResponse response, final Session session) throws ServletException,
      IOException {
    Map<String, Object> ret = new HashMap<>();
    FileSystem fs = null;
    try {
      try {
        fs = getFileSystem(username);
      } catch (final HadoopSecurityManagerException e) {
        errorAjax(response, ret, "Cannot get FileSystem.");
        return;
      }

      final String ajaxName = getParam(request, "ajax");
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

  private void handleAjaxFetchSchema(final FileSystem fs, final HttpServletRequest req,
      final Map<String, Object> ret, final Session session, final Path path) throws IOException,
      ServletException {
    HdfsFileViewer fileViewer = null;
    try {
      if (hasParam(req, "viewerId")) {
        fileViewer = this.viewers.get(getIntParam(req, "viewerId"));
        if (!fileViewer.getCapabilities(fs, path).contains(Capability.SCHEMA)) {
          fileViewer = null;
        }
      } else {
        for (final HdfsFileViewer viewer : this.viewers) {
          if (viewer.getCapabilities(fs, path).contains(Capability.SCHEMA)) {
            fileViewer = viewer;
          }
        }
      }
    } catch (final AccessControlException e) {
      ret.put("error", "Permission denied.");
    }

    if (fileViewer == null) {
      ret.put("error", "No viewers can display schema.");
      return;
    }
    ret.put("schema", fileViewer.getSchema(fs, path));
  }

  private void handleAjaxFetchFile(final FileSystem fs, final HttpServletRequest req,
      final HttpServletResponse resp, final Session session, final Path path) throws IOException,
      ServletException {
    final int startLine = getIntParam(req, "startLine", this.defaultStartLine);
    final int endLine = getIntParam(req, "endLine", this.defaultEndLine);
    final OutputStream output = resp.getOutputStream();

    if (endLine < startLine) {
      output.write(("Invalid range: endLine < startLine.").getBytes("UTF-8"));
      return;
    }

    if (endLine - startLine > this.fileMaxLines) {
      output.write(("Invalid range: range exceeds max number of lines.")
          .getBytes("UTF-8"));
      return;
    }

    // Use registered viewers to show the file content
    HdfsFileViewer fileViewer = null;
    try {
      if (hasParam(req, "viewerId")) {
        fileViewer = this.viewers.get(getIntParam(req, "viewerId"));
        if (!fileViewer.getCapabilities(fs, path).contains(Capability.READ)) {
          fileViewer = null;
        }
      } else {
        for (final HdfsFileViewer viewer : this.viewers) {
          if (viewer.getCapabilities(fs, path).contains(Capability.READ)) {
            fileViewer = viewer;
            break;
          }
        }
      }
      // use default text viewer
      if (fileViewer == null) {
        if (this.defaultViewer.getCapabilities(fs, path).contains(Capability.READ)) {
          fileViewer = this.defaultViewer;
        } else {
          output.write(("No viewer available for file.").getBytes("UTF-8"));
          return;
        }
      }
    } catch (final AccessControlException e) {
      output.write(("Permission denied.").getBytes("UTF-8"));
    }

    fileViewer.displayFile(fs, path, output, startLine, endLine);
  }
}
