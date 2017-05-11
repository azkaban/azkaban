/*
 * Copyright 2017 LinkedIn Corp.
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

import azkaban.db.AzDBTestUtility;
import azkaban.db.AzkabanDataSource;
import azkaban.database.AzkabanDatabaseSetup;
import azkaban.db.DatabaseOperator;
import azkaban.db.DatabaseOperatorImpl;
import azkaban.flow.Flow;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.utils.Md5Hasher;
import azkaban.utils.Props;
import azkaban.utils.Triple;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class JdbcProjectImplTest {

  private static final String SAMPLE_FILE = "sample_flow_01.zip";
  private ProjectLoader loader;
  private static DatabaseOperator dbOperator;
  private static Props props = new Props();

  @BeforeClass
  public static void setUp() throws Exception {
    AzkabanDataSource dataSource = new AzDBTestUtility.EmbeddedH2BasicDataSource();
    dbOperator = new DatabaseOperatorImpl(new QueryRunner(dataSource));

    String sqlScriptsDir = new File("../azkaban-db/src/main/sql/").getCanonicalPath();
    props.put("database.sql.scripts.dir", sqlScriptsDir);

    // TODO kunkun-tang: Need to refactor AzkabanDatabaseSetup to accept datasource in azakaban-db
    final azkaban.database.AzkabanDataSource dataSourceForSetupDB =
        new azkaban.database.AzkabanConnectionPoolTest.EmbeddedH2BasicDataSource();
    AzkabanDatabaseSetup setup = new AzkabanDatabaseSetup(dataSourceForSetupDB, props);
    setup.loadTableInfo();
    setup.updateDatabase(true, false);
  }

  @Before
  public void setup() {
    loader = new JdbcProjectImpl(props, dbOperator);
  }

  private void createThreeProjects() {
    String projectName = "mytestProject";
    String projectDescription = "This is my new project";
    User user = new User("testUser1");
    loader.createNewProject(projectName, projectDescription, user);
    String projectName2 = "mytestProject2";
    String projectDescription2 = "This is my new project2";
    loader.createNewProject(projectName2, projectDescription2, user);
    String projectName3 = "mytestProject3";
    String projectDescription3 = "This is my new project3";
    User user2 = new User("testUser2");
    loader.createNewProject(projectName3, projectDescription3, user2);
  }

  @Test
  public void testCreateProject() throws Exception {
    String projectName = "mytestProject";
    String projectDescription = "This is my new project";
    User user = new User("testUser1");
    Project project = loader.createNewProject(projectName, projectDescription, user);
    Assert.assertEquals(project.getName(), projectName);
    Assert.assertEquals(project.getDescription(), projectDescription);
    Assert.assertEquals(project.getLastModifiedUser(), "testUser1");
  }

  @Test
  public void testFetchAllActiveProjects() throws Exception {
    createThreeProjects();
    List<Project> projectList = loader.fetchAllActiveProjects();
    Assert.assertEquals(projectList.size(), 3);
  }

  @Test
  public void testFetchProjectByName() throws Exception {
    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    Assert.assertEquals(project.getName(), "mytestProject");
    Assert.assertEquals(project.getDescription(), "This is my new project");
    Assert.assertEquals(project.getLastModifiedUser(), "testUser1");
  }

  @Test
  public void testFetchProjectById() throws Exception {
    createThreeProjects();
    Project project1 = loader.fetchProjectByName("mytestProject");
    Project project2 = loader.fetchProjectById(project1.getId());
    Assert.assertEquals(project1.getName(), project2.getName());
    Assert.assertEquals(project1.getDescription(), project2.getDescription());
    Assert.assertEquals(project1.getLastModifiedUser(), project2.getLastModifiedUser());
  }

  @Test
  public void testUploadProjectFile() throws Exception {
    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    File testFile = new File(getClass().getClassLoader().getResource(SAMPLE_FILE).getFile());
    int newVersion = loader.getLatestProjectVersion(project) + 1;
    loader.uploadProjectFile(project.getId(), newVersion, testFile, "uploadUser1");

    ProjectFileHandler fileHandler = loader.getUploadedFile(project.getId(), newVersion);
    Assert.assertEquals(fileHandler.getFileName(), SAMPLE_FILE);
    Assert.assertEquals(fileHandler.getUploader(), "uploadUser1");
  }

  @Test(expected = ProjectManagerException.class)
  public void testDuplicateUploadProjectFile() throws Exception {
    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    File testFile = new File(getClass().getClassLoader().getResource(SAMPLE_FILE).getFile());
    int newVersion = loader.getLatestProjectVersion(project) + 1;
    loader.uploadProjectFile(project.getId(), newVersion, testFile, "uploadUser1");
    loader.uploadProjectFile(project.getId(), newVersion, testFile, "uploadUser1");
  }

  private byte[] computeHash(File localFile) {
    byte[] md5;
    try {
      md5 = Md5Hasher.md5Hash(localFile);
    } catch (IOException e) {
      throw new ProjectManagerException("Error getting md5 hash.", e);
    }
    return md5;
  }

  @Test
  public void testAddProjectVersion() throws Exception {
    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    File testFile = new File(getClass().getClassLoader().getResource(SAMPLE_FILE).getFile());
    int newVersion = loader.getLatestProjectVersion(project) + 1;
    loader.addProjectVersion(project.getId(), newVersion, testFile, "uploadUser1", computeHash(testFile), "resourceId1");
    int currVersion = loader.getLatestProjectVersion(project);
    Assert.assertEquals(currVersion, newVersion);
  }

  @Test
  public void testFetchProjectMetaData() throws Exception {
    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    File testFile = new File(getClass().getClassLoader().getResource(SAMPLE_FILE).getFile());
    int newVersion = loader.getLatestProjectVersion(project) + 1;
    loader.uploadProjectFile(project.getId(), newVersion, testFile, "uploadUser1");
    ProjectFileHandler pfh = loader.fetchProjectMetaData(project.getId(), newVersion);
    Assert.assertEquals(pfh.getVersion(), newVersion);
  }

  @Test
  public void testChangeProjectVersion() throws Exception {
    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    int newVersion = loader.getLatestProjectVersion(project) + 7;
    loader.changeProjectVersion(project, newVersion, "uploadUser1");
    Project sameProject= loader.fetchProjectById(project.getId());
    Assert.assertEquals(sameProject.getVersion(), newVersion);
  }

  @Test
  public void testUpdatePermission() throws Exception {
    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    loader.updatePermission(project, project.getLastModifiedUser(), new Permission(Permission.Type.ADMIN), false);

    List<Triple<String, Boolean, Permission>> permissionsTriple = loader.getProjectPermissions(project);
    Assert.assertEquals(permissionsTriple.size(), 1);
    Assert.assertEquals(permissionsTriple.get(0).getFirst(), "testUser1");
    Assert.assertEquals(permissionsTriple.get(0).getThird().toString(), "ADMIN");
  }

  @Test
  public void testUpdateProjectSettings() throws Exception {
    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    Assert.assertEquals(project.getProxyUsers().size(), 0);
    project.addProxyUser("ProxyUser");
    loader.updateProjectSettings(project);
    Project sameProject = loader.fetchProjectByName("mytestProject");
    Assert.assertEquals(sameProject.getProxyUsers().size(), 1);
  }

  @Test
  public void testRemovePermission() throws Exception {
    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    loader.updatePermission(project, project.getLastModifiedUser(), new Permission(Permission.Type.ADMIN), false);
    loader.removePermission(project, project.getLastModifiedUser(), false);
    List<Triple<String, Boolean, Permission>> permissionsTriple = loader.getProjectPermissions(project);
    Assert.assertEquals(permissionsTriple.size(), 0);
  }

  @Test
  public void testRemoveProject() throws Exception {
    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    Assert.assertEquals(project.isActive(), true);
    loader.removeProject(project, "testUser1");
    Project removedProject = loader.fetchProjectByName("mytestProject");
    Assert.assertEquals(removedProject.isActive(), false);
  }

  @Test
  public void testPostAndGetEvent() throws Exception {
    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    loader.postEvent(project, ProjectLogEvent.EventType.CREATED, "testUser1", "create a message bla");
    List<ProjectLogEvent> events = loader.getProjectEvents(project, 5, 0);
    Assert.assertEquals(events.size(), 1);
    Assert.assertEquals(events.get(0).getMessage(), "create a message bla");
  }

  @Test
  public void testUpdateDescription() throws Exception {
    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    loader.updateDescription(project, "new Description bla", "testUser1");
    Project sameProject = loader.fetchProjectByName("mytestProject");
    Assert.assertEquals(sameProject.getDescription(), "new Description bla");
  }

  @Test
  public void testUploadAndFetchFlow() throws Exception {
    Flow flow1 = new Flow("flow1");
    Flow flow2 = new Flow("flow2");
    List<Flow> flowList = Arrays.asList(flow1, flow2);

    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    loader.uploadFlows(project, project.getVersion(), flowList);

    List<Flow> flowList2 = loader.fetchAllProjectFlows(project);
    Assert.assertEquals(flowList2.size(), 2);
  }

  @Test
  public void testUpdateFlow() throws Exception {
    Flow flow1 = new Flow("flow1");
    List<Flow> flowList = Collections.singletonList(flow1);

    flow1.setLayedOut(false);
    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    loader.uploadFlows(project, project.getVersion(), flowList);

    flow1.setLayedOut(true);
    loader.updateFlow(project, project.getVersion(), flow1);
    List<Flow> flowList2 = loader.fetchAllProjectFlows(project);
    Assert.assertEquals(flowList2.get(0).isLayedOut(), true);
  }

  @Test
  public void testUploadOrUpdateProjectProperty() throws Exception {
    Props props = new Props();
    props.setSource("source1");
    props.put("key1", "value1");
    props.put("key2", "value2");

    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    loader.uploadProjectProperty(project, props);

    Props sameProps = loader.fetchProjectProperty(project, props.getSource());
    Assert.assertEquals(sameProps.get("key1"), "value1");
    Assert.assertEquals(sameProps.get("key2"), "value2");

    props.put("key2", "value9");
    loader.updateProjectProperty(project, props);

    Props sameProps2 = loader.fetchProjectProperty(project, props.getSource());
    Assert.assertEquals(sameProps2.get("key2"), "value9");
  }

  @Test
  public void testFetchProjectProperties() throws Exception {
    Props props1 = new Props();
    props1.setSource("source1");
    props1.put("key1", "value1");
    props1.put("key2", "value2");

    Props props2 = new Props();
    props2.setSource("source2");
    props2.put("keykey", "valuevalue1");
    props2.put("keyaaa", "valueaaa");
    List<Props> list = Arrays.asList(props1, props2);

    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    loader.uploadProjectProperties(project, list);

    Map<String, Props> propsMap = loader.fetchProjectProperties(project.getId(), project.getVersion());
    Assert.assertEquals(propsMap.get("source1").get("key2"), "value2");
    Assert.assertEquals(propsMap.get("source2").get("keyaaa"), "valueaaa");
  }

  @Test
  public void cleanOlderProjectVersion() throws Exception {
    createThreeProjects();
    Project project = loader.fetchProjectByName("mytestProject");
    File testFile = new File(getClass().getClassLoader().getResource(SAMPLE_FILE).getFile());
    int newVersion = loader.getLatestProjectVersion(project) + 1;
    loader.uploadProjectFile(project.getId(), newVersion, testFile, "uploadUser1");

    ProjectFileHandler fileHandler = loader.getUploadedFile(project.getId(), newVersion);
    Assert.assertEquals(fileHandler.getNumChunks(), 1);

    loader.cleanOlderProjectVersion(project.getId(), newVersion+1);

    ProjectFileHandler fileHandler2 = loader.fetchProjectMetaData(project.getId(), newVersion);
    Assert.assertEquals(fileHandler2.getNumChunks(), 0);
  }

  @After
  public void clearDB() {
    try {
      dbOperator.update("DELETE FROM projects");
      dbOperator.update("DELETE FROM project_versions");
      dbOperator.update("DELETE FROM project_properties");
      dbOperator.update("DELETE FROM project_permissions");
      dbOperator.update("DELETE FROM project_flows");
      dbOperator.update("DELETE FROM project_files");
      dbOperator.update("DELETE FROM project_events");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void destroyDB() {
    try {
      dbOperator.update("DROP ALL OBJECTS");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
