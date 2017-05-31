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

package azkaban.project;

import azkaban.database.DataSourceUtils;
import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.Node;
import azkaban.project.ProjectLogEvent.EventType;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import javax.sql.DataSource;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class JdbcProjectLoaderTest {

  private static final String host = "localhost";
  private static final int port = 3306;
  private static final String database = "test";
  private static final String user = "azkaban";
  private static final String password = "azkaban";
  private static final int numConnections = 10;
  private static boolean testDBExists;

  @BeforeClass
  public static void setupDB() {
    final DataSource dataSource =
        DataSourceUtils.getMySQLDataSource(host, port, database, user,
            password, numConnections);
    testDBExists = true;

    Connection connection = null;
    try {
      connection = dataSource.getConnection();
    } catch (final SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    final CountHandler countHandler = new CountHandler();
    final QueryRunner runner = new QueryRunner();
    try {
      runner.query(connection, "SELECT COUNT(1) FROM projects", countHandler);
    } catch (final SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.query(connection, "SELECT COUNT(1) FROM project_events",
          countHandler);
    } catch (final SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.query(connection, "SELECT COUNT(1) FROM project_permissions",
          countHandler);
    } catch (final SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.query(connection, "SELECT COUNT(1) FROM project_files",
          countHandler);
    } catch (final SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.query(connection, "SELECT COUNT(1) FROM project_flows",
          countHandler);
    } catch (final SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.query(connection, "SELECT COUNT(1) FROM project_properties",
          countHandler);
    } catch (final SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    DbUtils.closeQuietly(connection);

    clearDB();
  }

  private static void clearDB() {
    if (!testDBExists) {
      return;
    }

    final DataSource dataSource =
        DataSourceUtils.getMySQLDataSource(host, port, database, user,
            password, numConnections);
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
    } catch (final SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    final QueryRunner runner = new QueryRunner();
    try {
      runner.update(connection, "DELETE FROM projects");

    } catch (final SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.update(connection, "DELETE FROM project_events");
    } catch (final SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.update(connection, "DELETE FROM project_permissions");
    } catch (final SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.update(connection, "DELETE FROM project_files");
    } catch (final SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.update(connection, "DELETE FROM project_flows");
    } catch (final SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    try {
      runner.update(connection, "DELETE FROM project_properties");
    } catch (final SQLException e) {
      e.printStackTrace();
      testDBExists = false;
      DbUtils.closeQuietly(connection);
      return;
    }

    DbUtils.closeQuietly(connection);
  }

  /**
   * Test case to validated permissions for fetchProjectByName
   **/
  @Test
  public void testPermissionRetrivalByFetchProjectByName()
      throws ProjectManagerException {
    if (!isTestSetup()) {
      return;
    }

    final ProjectLoader loader = createLoader();
    final String projectName = "mytestProject";
    final String projectDescription = "This is my new project";
    final User user = new User("testUser");

    final Project project =
        loader.createNewProject(projectName, projectDescription, user);

    final Permission perm = new Permission(0x2);
    loader.updatePermission(project, user.getUserId(), perm, false);
    loader.updatePermission(project, "group", perm, true);

    final Permission permOverride = new Permission(0x6);
    loader.updatePermission(project, user.getUserId(), permOverride, false);

    final Project fetchedProject = loader.fetchProjectByName(project.getName());
    assertProjectMemberEquals(project, fetchedProject);
    Assert.assertEquals(permOverride,
        fetchedProject.getUserPermission(user.getUserId()));
  }

  /**
   * Default Test case for fetchProjectByName
   **/
  @Test
  public void testProjectRetrievalByFetchProjectByName()
      throws ProjectManagerException {
    if (!isTestSetup()) {
      return;
    }

    final ProjectLoader loader = createLoader();
    final String projectName = "mytestProject";
    final String projectDescription = "This is my new project";
    final User user = new User("testUser");

    final Project project =
        loader.createNewProject(projectName, projectDescription, user);

    final Project fetchedProject = loader.fetchProjectByName(project.getName());
    assertProjectMemberEquals(project, fetchedProject);
  }

  /**
   * Default Test case for fetchProjectByName
   **/
  @Test
  public void testDuplicateRetrivalByFetchProjectByName()
      throws ProjectManagerException {
    if (!isTestSetup()) {
      return;
    }

    final ProjectLoader loader = createLoader();
    final String projectName = "mytestProject";
    final String projectDescription = "This is my new project";
    final User user = new User("testUser");

    final Project project =
        loader.createNewProject(projectName, projectDescription, user);

    loader.removeProject(project, user.getUserId());

    final Project newProject =
        loader.createNewProject(projectName, projectDescription, user);

    final Project fetchedProject = loader.fetchProjectByName(project.getName());
    Assert.assertEquals(newProject.getId(), fetchedProject.getId());

  }

  /**
   * Test case for NonExistantProject project fetch
   **/
  @Test
  public void testInvalidProjectByFetchProjectByName() {
    if (!isTestSetup()) {
      return;
    }
    final ProjectLoader loader = createLoader();
    try {
      loader.fetchProjectByName("NonExistantProject");
    } catch (final ProjectManagerException ex) {
      System.out.println("Test true");
    }
    Assert.fail("Expecting exception, but didn't get one");
  }

  @Test
  public void testCreateProject() throws ProjectManagerException {
    if (!isTestSetup()) {
      return;
    }

    final ProjectLoader loader = createLoader();
    final String projectName = "mytestProject";
    final String projectDescription = "This is my new project";
    final User user = new User("testUser");

    final Project project =
        loader.createNewProject(projectName, projectDescription, user);
    Assert.assertTrue("Project Id set", project.getId() > -1);
    Assert.assertEquals("Project name", projectName, project.getName());
    Assert.assertEquals("Project description", projectDescription,
        project.getDescription());

    System.out.println("Test true");
    final Project project2 = loader.fetchProjectById(project.getId());
    assertProjectMemberEquals(project, project2);
  }

  @Test
  public void testRemoveProject() throws ProjectManagerException {
    if (!isTestSetup()) {
      return;
    }

    final ProjectLoader loader = createLoader();
    final String projectName = "testRemoveProject";
    final String projectDescription = "This is my new project";
    final User user = new User("testUser");

    final Project project =
        loader.createNewProject(projectName, projectDescription, user);
    Assert.assertTrue("Project Id set", project.getId() > -1);
    Assert.assertEquals("Project name", projectName, project.getName());
    Assert.assertEquals("Project description", projectDescription,
        project.getDescription());

    final Project project2 = loader.fetchProjectById(project.getId());
    assertProjectMemberEquals(project, project2);
    loader.removeProject(project, user.getUserId());

    final Project project3 = loader.fetchProjectById(project.getId());
    Assert.assertFalse(project3.isActive());

    final List<Project> projList = loader.fetchAllActiveProjects();
    for (final Project proj : projList) {
      Assert.assertTrue(proj.getId() != project.getId());
    }
  }

  @Test
  public void testAddRemovePermissions() throws ProjectManagerException {
    if (!isTestSetup()) {
      return;
    }

    final ProjectLoader loader = createLoader();
    final String projectName = "mytestProject1";
    final String projectDescription = "This is my new project";
    final User user = new User("testUser");

    final Project project =
        loader.createNewProject(projectName, projectDescription, user);
    Assert.assertTrue("Project Id set", project.getId() > -1);
    Assert.assertEquals("Project name", projectName, project.getName());
    Assert.assertEquals("Project description", projectDescription,
        project.getDescription());

    final Permission perm = new Permission(0x2);
    loader.updatePermission(project, user.getUserId(), new Permission(0x2),
        false);
    loader.updatePermission(project, "group1", new Permission(0x2), true);
    Assert.assertEquals(perm, project.getUserPermission(user.getUserId()));

    final Permission permOverride = new Permission(0x6);
    loader.updatePermission(project, user.getUserId(), permOverride, false);
    Assert.assertEquals(permOverride,
        project.getUserPermission(user.getUserId()));

    final Project project2 = loader.fetchProjectById(project.getId());
    assertProjectMemberEquals(project, project2);
    Assert.assertEquals(permOverride,
        project2.getUserPermission(user.getUserId()));
  }

  @Test
  public void testProjectEventLogs() throws ProjectManagerException {
    if (!isTestSetup()) {
      return;
    }

    final ProjectLoader loader = createLoader();
    final String projectName = "testProjectEventLogs";
    final String projectDescription = "This is my new project";
    final User user = new User("testUser");

    final String message = "My message";
    final EventType type = EventType.USER_PERMISSION;
    final Project project =
        loader.createNewProject(projectName, projectDescription, user);
    loader.postEvent(project, type, user.getUserId(), message);

    final List<ProjectLogEvent> events = loader.getProjectEvents(project, 10, 0);
    Assert.assertTrue(events.size() == 1);

    final ProjectLogEvent event = events.get(0);
    Assert.assertEquals(event.getProjectId(), project.getId());
    Assert.assertEquals(event.getUser(), user.getUserId());
    Assert.assertEquals(event.getMessage(), message);
    Assert.assertEquals(event.getType(), type);
  }

  @Ignore
  @Test
  public void testFlowUpload() throws ProjectManagerException {
    final ProjectLoader loader = createLoader();
    ((JdbcProjectLoader) loader)
        .setDefaultEncodingType(JdbcProjectLoader.EncodingType.GZIP);
    final String projectName = "mytestFlowUpload1";
    final String projectDescription = "This is my new project";
    final User user = new User("testUser");

    final Project project =
        loader.createNewProject(projectName, projectDescription, user);

    final Flow flow = new Flow("MyNewFlow");

    flow.addNode(new Node("A"));
    flow.addNode(new Node("B"));
    flow.addNode(new Node("C"));
    flow.addNode(new Node("D"));

    flow.addEdge(new Edge("A", "B"));
    flow.addEdge(new Edge("A", "C"));
    flow.addEdge(new Edge("B", "D"));
    flow.addEdge(new Edge("C", "D"));

    flow.initialize();

    loader.uploadFlow(project, 4, flow);
    project.setVersion(4);
    final Flow newFlow = loader.fetchFlow(project, flow.getId());
    Assert.assertTrue(newFlow != null);
    Assert.assertEquals(flow.getId(), newFlow.getId());
    Assert.assertEquals(flow.getEdges().size(), newFlow.getEdges().size());
    Assert.assertEquals(flow.getNodes().size(), newFlow.getNodes().size());
  }

  @Ignore
  @Test
  public void testFlowUploadPlain() throws ProjectManagerException {
    final ProjectLoader loader = createLoader();
    ((JdbcProjectLoader) loader)
        .setDefaultEncodingType(JdbcProjectLoader.EncodingType.PLAIN);
    final String projectName = "mytestFlowUpload2";
    final String projectDescription = "This is my new project";
    final User user = new User("testUser");

    final Project project =
        loader.createNewProject(projectName, projectDescription, user);

    final Flow flow = new Flow("MyNewFlow2");

    flow.addNode(new Node("A1"));
    flow.addNode(new Node("B1"));
    flow.addNode(new Node("C1"));
    flow.addNode(new Node("D1"));

    flow.addEdge(new Edge("A1", "B1"));
    flow.addEdge(new Edge("A1", "C1"));
    flow.addEdge(new Edge("B1", "D1"));
    flow.addEdge(new Edge("C1", "D1"));

    flow.initialize();

    loader.uploadFlow(project, 4, flow);
    project.setVersion(4);
    final Flow newFlow = loader.fetchFlow(project, flow.getId());
    Assert.assertTrue(newFlow != null);
    Assert.assertEquals(flow.getId(), newFlow.getId());
    Assert.assertEquals(flow.getEdges().size(), newFlow.getEdges().size());
    Assert.assertEquals(flow.getNodes().size(), newFlow.getNodes().size());

    final List<Flow> flows = loader.fetchAllProjectFlows(project);
    Assert.assertTrue(flows.size() == 1);
  }

  @Ignore
  @Test
  public void testProjectProperties() throws ProjectManagerException {
    final ProjectLoader loader = createLoader();
    ((JdbcProjectLoader) loader)
        .setDefaultEncodingType(JdbcProjectLoader.EncodingType.PLAIN);
    final String projectName = "testProjectProperties";
    final String projectDescription = "This is my new project";
    final User user = new User("testUser");

    final Project project =
        loader.createNewProject(projectName, projectDescription, user);
    project.setVersion(5);
    final Props props = new Props();
    props.put("a", "abc");
    props.put("b", "bcd");
    props.put("c", "cde");
    props.setSource("mysource");
    loader.uploadProjectProperty(project, props);

    final Props retProps = loader.fetchProjectProperty(project, "mysource");

    Assert.assertEquals(retProps.getSource(), props.getSource());
    Assert.assertEquals(retProps.getKeySet(), props.getKeySet());
    Assert.assertEquals(PropsUtils.toStringMap(retProps, true),
        PropsUtils.toStringMap(props, true));
  }

  @Test
  public void testProjectFilesUpload() throws ProjectManagerException {
    if (!isTestSetup()) {
      return;
    }

    final ProjectLoader loader = createLoader();
    final String projectName = "testProjectFilesUpload1";
    final String projectDescription = "This is my new project";
    final User user = new User("testUser");

    final Project project =
        loader.createNewProject(projectName, projectDescription, user);
    Assert.assertTrue("Project Id set", project.getId() > -1);
    Assert.assertEquals("Project name", projectName, project.getName());
    Assert.assertEquals("Project description", projectDescription,
        project.getDescription());

    final File testFile = new File("unit/project/testjob/testjob.zip");

    loader.uploadProjectFile(project.getId(), 1, testFile, user.getUserId());

    final ProjectFileHandler handler = loader.getUploadedFile(project.getId(), 1);
    Assert.assertEquals(handler.getProjectId(), project.getId());
    Assert.assertEquals(handler.getFileName(), "testjob.zip");
    Assert.assertEquals(handler.getVersion(), 1);
    Assert.assertEquals(handler.getFileType(), "zip");
    final File file = handler.getLocalFile();
    Assert.assertTrue(handler.getLocalFile().exists());
    Assert.assertEquals(handler.getFileName(), "testjob.zip");
    Assert.assertEquals(handler.getUploader(), user.getUserId());

    handler.deleteLocalFile();
    Assert.assertTrue(handler.getLocalFile() == null);
    Assert.assertFalse(file.exists());
  }

  // Custom equals for what I think is important
  private void assertProjectMemberEquals(final Project p1, final Project p2) {
    Assert.assertEquals(p1.getId(), p2.getId());
    Assert.assertEquals(p1.getName(), p2.getName());
    Assert.assertEquals(p1.getCreateTimestamp(), p2.getCreateTimestamp());
    Assert.assertEquals(p1.getDescription(), p2.getDescription());
    Assert.assertEquals(p1.getLastModifiedUser(), p2.getLastModifiedUser());
    Assert.assertEquals(p1.getVersion(), p2.getVersion());
    Assert.assertEquals(p1.isActive(), p2.isActive());
    Assert.assertEquals(p1.getLastModifiedUser(), p2.getLastModifiedUser());

    assertUserPermissionsEqual(p1, p2);
    assertGroupPermissionsEqual(p1, p2);
  }

  private void assertUserPermissionsEqual(final Project p1, final Project p2) {
    final List<Pair<String, Permission>> perm1 = p1.getUserPermissions();
    final List<Pair<String, Permission>> perm2 = p2.getUserPermissions();

    Assert.assertEquals(perm1.size(), perm2.size());

    {
      final HashMap<String, Permission> perm1Map = new HashMap<>();
      for (final Pair<String, Permission> p : perm1) {
        perm1Map.put(p.getFirst(), p.getSecond());
      }
      for (final Pair<String, Permission> p : perm2) {
        Assert.assertTrue(perm1Map.containsKey(p.getFirst()));
        final Permission perm = perm1Map.get(p.getFirst());
        Assert.assertEquals(perm, p.getSecond());
      }
    }

    {
      final HashMap<String, Permission> perm2Map = new HashMap<>();
      for (final Pair<String, Permission> p : perm2) {
        perm2Map.put(p.getFirst(), p.getSecond());
      }
      for (final Pair<String, Permission> p : perm1) {
        Assert.assertTrue(perm2Map.containsKey(p.getFirst()));
        final Permission perm = perm2Map.get(p.getFirst());
        Assert.assertEquals(perm, p.getSecond());
      }
    }
  }

  private void assertGroupPermissionsEqual(final Project p1, final Project p2) {
    final List<Pair<String, Permission>> perm1 = p1.getGroupPermissions();
    final List<Pair<String, Permission>> perm2 = p2.getGroupPermissions();

    Assert.assertEquals(perm1.size(), perm2.size());

    {
      final HashMap<String, Permission> perm1Map = new HashMap<>();
      for (final Pair<String, Permission> p : perm1) {
        perm1Map.put(p.getFirst(), p.getSecond());
      }
      for (final Pair<String, Permission> p : perm2) {
        Assert.assertTrue(perm1Map.containsKey(p.getFirst()));
        final Permission perm = perm1Map.get(p.getFirst());
        Assert.assertEquals(perm, p.getSecond());
      }
    }

    {
      final HashMap<String, Permission> perm2Map = new HashMap<>();
      for (final Pair<String, Permission> p : perm2) {
        perm2Map.put(p.getFirst(), p.getSecond());
      }
      for (final Pair<String, Permission> p : perm1) {
        Assert.assertTrue(perm2Map.containsKey(p.getFirst()));
        final Permission perm = perm2Map.get(p.getFirst());
        Assert.assertEquals(perm, p.getSecond());
      }
    }
  }

  private ProjectLoader createLoader() {
    final Props props = new Props();
    props.put("database.type", "mysql");

    props.put("mysql.host", host);
    props.put("mysql.port", port);
    props.put("mysql.user", user);
    props.put("mysql.database", database);
    props.put("mysql.password", password);
    props.put("mysql.numconnections", numConnections);

    return new JdbcProjectLoader(props);
  }

  private boolean isTestSetup() {
    if (!testDBExists) {
      System.err.println("Skipping DB test because Db not setup.");
      return false;
    }

    System.out.println("Running DB test because Db setup.");
    return true;
  }

  public static class CountHandler implements ResultSetHandler<Integer> {

    @Override
    public Integer handle(final ResultSet rs) throws SQLException {
      int val = 0;
      while (rs.next()) {
        val++;
      }

      return val;
    }
  }
}
