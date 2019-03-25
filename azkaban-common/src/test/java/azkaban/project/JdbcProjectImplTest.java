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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import azkaban.db.DatabaseOperator;
import azkaban.flow.Flow;
import azkaban.test.Utils;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.utils.Md5Hasher;
import azkaban.utils.Props;
import azkaban.utils.Triple;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class JdbcProjectImplTest {

  private static final String SAMPLE_FILE = "sample_flow_01.zip";
  private static final String BASIC_FLOW_YAML_DIR = "basicflowyamltest";
  private static final String LARGE_FLOW_YAML_DIR = "largeflowyamltest";
  private static final String BASIC_FLOW_FILE = "basic_flow.flow";
  private static final String LARGE_FLOW_FILE = "large_file.flow";
  private static final int PROJECT_ID = 123;
  private static final int PROJECT_VERSION = 3;
  private static final int FLOW_VERSION = 1;
  private static final Props props = new Props();
  private static DatabaseOperator dbOperator;
  private ProjectLoader loader;

  @BeforeClass
  public static void setUp() throws Exception {
    dbOperator = Utils.initTestDB();
  }

  @AfterClass
  public static void destroyDB() {
    try {
      dbOperator.update("DROP ALL OBJECTS");
      dbOperator.update("SHUTDOWN");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  @Before
  public void setup() {
    this.loader = new JdbcProjectImpl(props, dbOperator);
  }

  private void createThreeProjects() {
    final String projectName = "mytestProject";
    final String projectDescription = "This is my new project";
    final User user = new User("testUser1");
    this.loader.createNewProject(projectName, projectDescription, user);
    final String projectName2 = "mytestProject2";
    final String projectDescription2 = "This is my new project2";
    this.loader.createNewProject(projectName2, projectDescription2, user);
    final String projectName3 = "mytestProject3";
    final String projectDescription3 = "This is my new project3";
    final User user2 = new User("testUser2");
    this.loader.createNewProject(projectName3, projectDescription3, user2);
  }

  @Test
  public void testCreateProject() throws Exception {
    final String projectName = "mytestProject";
    final String projectDescription = "This is my new project";
    final User user = new User("testUser1");
    final Project project = this.loader.createNewProject(projectName, projectDescription, user);
    Assert.assertEquals(project.getName(), projectName);
    Assert.assertEquals(project.getDescription(), projectDescription);
    Assert.assertEquals(project.getLastModifiedUser(), "testUser1");
  }

  @Test
  public void testCreateProjectsWithDifferentCases() {
    final String projectName = "mytestproject";
    final String projectDescription = "This is my new project with lower cases.";
    final User user = new User("testUser1");
    this.loader.createNewProject(projectName, projectDescription, user);
    final String projectName2 = "MYTESTPROJECT";
    final String projectDescription2 = "This is my new project with UPPER CASES.";
    assertThatThrownBy(
        () -> this.loader.createNewProject(projectName2, projectDescription2, user))
        .isInstanceOf(ProjectManagerException.class)
        .hasMessageContaining(
            "Active project with name " + projectName2 + " already exists in db.");
  }

  @Test
  public void testFetchAllActiveProjects() throws Exception {
    createThreeProjects();
    final List<Project> projectList = this.loader.fetchAllActiveProjects();
    Assert.assertEquals(projectList.size(), 3);
  }

  @Test
  public void testFetchProjectByName() throws Exception {
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    Assert.assertEquals(project.getName(), "mytestProject");
    Assert.assertEquals(project.getDescription(), "This is my new project");
    Assert.assertEquals(project.getLastModifiedUser(), "testUser1");
  }

  @Test
  public void testFetchProjectById() throws Exception {
    createThreeProjects();
    final Project project1 = this.loader.fetchProjectByName("mytestProject");
    final Project project2 = this.loader.fetchProjectById(project1.getId());
    Assert.assertEquals(project1.getName(), project2.getName());
    Assert.assertEquals(project1.getDescription(), project2.getDescription());
    Assert.assertEquals(project1.getLastModifiedUser(), project2.getLastModifiedUser());
  }

  @Test
  public void testUploadProjectFile() throws Exception {
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    final File testFile = new File(getClass().getClassLoader().getResource(SAMPLE_FILE).getFile());
    final int newVersion = this.loader.getLatestProjectVersion(project) + 1;
    this.loader.uploadProjectFile(project.getId(), newVersion, testFile, "uploadUser1");

    final ProjectFileHandler fileHandler = this.loader.getUploadedFile(project.getId(), newVersion);
    Assert.assertEquals(fileHandler.getFileName(), SAMPLE_FILE);
    Assert.assertEquals(fileHandler.getUploader(), "uploadUser1");
  }

  @Test(expected = ProjectManagerException.class)
  public void testDuplicateUploadProjectFile() throws Exception {
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    final File testFile = new File(getClass().getClassLoader().getResource(SAMPLE_FILE).getFile());
    final int newVersion = this.loader.getLatestProjectVersion(project) + 1;
    this.loader.uploadProjectFile(project.getId(), newVersion, testFile, "uploadUser1");
    this.loader.uploadProjectFile(project.getId(), newVersion, testFile, "uploadUser1");
  }

  private byte[] computeHash(final File localFile) {
    final byte[] md5;
    try {
      md5 = Md5Hasher.md5Hash(localFile);
    } catch (final IOException e) {
      throw new ProjectManagerException("Error getting md5 hash.", e);
    }
    return md5;
  }

  @Test
  public void testAddProjectVersion() throws Exception {
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    final File testFile = new File(getClass().getClassLoader().getResource(SAMPLE_FILE).getFile());
    final int newVersion = this.loader.getLatestProjectVersion(project) + 1;
    this.loader.addProjectVersion(project.getId(), newVersion, testFile, "uploadUser1",
        computeHash(testFile), "resourceId1");
    final int currVersion = this.loader.getLatestProjectVersion(project);
    Assert.assertEquals(currVersion, newVersion);
  }

  @Test
  public void testFetchProjectMetaData() throws Exception {
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    final File testFile = new File(getClass().getClassLoader().getResource(SAMPLE_FILE).getFile());
    final int newVersion = this.loader.getLatestProjectVersion(project) + 1;
    this.loader.uploadProjectFile(project.getId(), newVersion, testFile, "uploadUser1");
    final ProjectFileHandler pfh = this.loader.fetchProjectMetaData(project.getId(), newVersion);
    Assert.assertEquals(pfh.getVersion(), newVersion);
  }

  @Test
  public void testChangeProjectVersion() throws Exception {
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    final int newVersion = this.loader.getLatestProjectVersion(project) + 7;
    this.loader.changeProjectVersion(project, newVersion, "uploadUser1");
    final Project sameProject = this.loader.fetchProjectById(project.getId());
    Assert.assertEquals(sameProject.getVersion(), newVersion);
  }

  @Test
  public void testUpdatePermission() throws Exception {
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    this.loader.updatePermission(project, project.getLastModifiedUser(),
        new Permission(Permission.Type.ADMIN), false);

    final List<Triple<String, Boolean, Permission>> permissionsTriple = this.loader
        .getProjectPermissions(project);
    Assert.assertEquals(permissionsTriple.size(), 1);
    Assert.assertEquals(permissionsTriple.get(0).getFirst(), "testUser1");
    Assert.assertEquals(permissionsTriple.get(0).getThird().toString(), "ADMIN");
  }

  @Test
  public void testUpdateProjectSettings() throws Exception {
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    Assert.assertEquals(project.getProxyUsers().size(), 0);
    project.addProxyUser("ProxyUser");
    this.loader.updateProjectSettings(project);
    final Project sameProject = this.loader.fetchProjectByName("mytestProject");
    Assert.assertEquals(sameProject.getProxyUsers().size(), 1);
  }

  @Test
  public void testRemovePermission() throws Exception {
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    this.loader.updatePermission(project, project.getLastModifiedUser(),
        new Permission(Permission.Type.ADMIN), false);
    this.loader.removePermission(project, project.getLastModifiedUser(), false);
    final List<Triple<String, Boolean, Permission>> permissionsTriple = this.loader
        .getProjectPermissions(project);
    Assert.assertEquals(permissionsTriple.size(), 0);
  }

  @Test
  public void testRemoveProject() throws Exception {
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    Assert.assertEquals(project.isActive(), true);
    this.loader.removeProject(project, "testUser1");
    final Project removedProject = this.loader.fetchProjectByName("mytestProject");
    Assert.assertEquals(removedProject.isActive(), false);
  }

  @Test
  public void testPostAndGetEvent() throws Exception {
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    this.loader
        .postEvent(project, ProjectLogEvent.EventType.CREATED, "testUser1", "create a message bla");
    final List<ProjectLogEvent> events = this.loader.getProjectEvents(project, 5, 0);
    Assert.assertEquals(events.size(), 1);
    Assert.assertEquals(events.get(0).getMessage(), "create a message bla");
  }

  @Test
  public void testUpdateDescription() throws Exception {
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    this.loader.updateDescription(project, "new Description bla", "testUser1");
    final Project sameProject = this.loader.fetchProjectByName("mytestProject");
    Assert.assertEquals(sameProject.getDescription(), "new Description bla");
  }

  @Test
  public void testUploadAndFetchFlow() throws Exception {
    final Flow flow1 = new Flow("flow1");
    final Flow flow2 = new Flow("flow2");
    final List<Flow> flowList = Arrays.asList(flow1, flow2);

    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    this.loader.uploadFlows(project, project.getVersion(), flowList);

    final List<Flow> flowList2 = this.loader.fetchAllProjectFlows(project);
    Assert.assertEquals(flowList2.size(), 2);
  }

  @Test
  public void testUpdateFlow() throws Exception {
    final Flow flow1 = new Flow("flow1");
    final List<Flow> flowList = Collections.singletonList(flow1);

    flow1.setLayedOut(false);
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    this.loader.uploadFlows(project, project.getVersion(), flowList);

    flow1.setLayedOut(true);
    this.loader.updateFlow(project, project.getVersion(), flow1);
    final List<Flow> flowList2 = this.loader.fetchAllProjectFlows(project);
    Assert.assertEquals(flowList2.get(0).isLayedOut(), true);
  }

  @Test
  public void testUploadOrUpdateProjectProperty() throws Exception {
    final Props props = new Props();
    props.setSource("source1");
    props.put("key1", "value1");
    props.put("key2", "value2");

    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    this.loader.uploadProjectProperty(project, props);

    final Props sameProps = this.loader.fetchProjectProperty(project, props.getSource());
    Assert.assertEquals(sameProps.get("key1"), "value1");
    Assert.assertEquals(sameProps.get("key2"), "value2");

    props.put("key2", "value9");
    this.loader.updateProjectProperty(project, props);

    final Props sameProps2 = this.loader.fetchProjectProperty(project, props.getSource());
    Assert.assertEquals(sameProps2.get("key2"), "value9");
  }

  @Test
  public void testFetchProjectProperties() throws Exception {
    final Props props1 = new Props();
    props1.setSource("source1");
    props1.put("key1", "value1");
    props1.put("key2", "value2");

    final Props props2 = new Props();
    props2.setSource("source2");
    props2.put("keykey", "valuevalue1");
    props2.put("keyaaa", "valueaaa");
    final List<Props> list = Arrays.asList(props1, props2);

    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    this.loader.uploadProjectProperties(project, list);

    final Map<String, Props> propsMap = this.loader
        .fetchProjectProperties(project.getId(), project.getVersion());
    Assert.assertEquals(propsMap.get("source1").get("key2"), "value2");
    Assert.assertEquals(propsMap.get("source2").get("keyaaa"), "valueaaa");
  }

  @Test
  public void cleanOlderProjectVersion() {
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    final File testFile = new File(getClass().getClassLoader().getResource(SAMPLE_FILE).getFile());
    final int newVersion = this.loader.getLatestProjectVersion(project) + 1;
    this.loader.uploadProjectFile(project.getId(), newVersion, testFile, "uploadUser1");

    final ProjectFileHandler fileHandler = this.loader.getUploadedFile(project.getId(), newVersion);
    Assert.assertEquals(fileHandler.getNumChunks(), 1);
    assertNumChunks(project, newVersion, 1);

    this.loader.cleanOlderProjectVersion(project.getId(), newVersion + 1, Collections.emptyList());

    assertNumChunks(project, newVersion, 0);
    assertGetUploadedFileOfCleanedVersion(project.getId(), newVersion);
  }

  @Test
  public void cleanOlderProjectVersionExcludedVersion() {
    createThreeProjects();
    final Project project = this.loader.fetchProjectByName("mytestProject");
    final File testFile = new File(getClass().getClassLoader().getResource(SAMPLE_FILE).getFile());
    final int newVersion = this.loader.getLatestProjectVersion(project) + 1;
    this.loader.uploadProjectFile(project.getId(), newVersion, testFile, "uploadUser1");
    final int newVersion2 = this.loader.getLatestProjectVersion(project) + 1;
    this.loader.uploadProjectFile(project.getId(), newVersion2, testFile, "uploadUser1");
    this.loader.cleanOlderProjectVersion(project.getId(), newVersion2 + 1,
        Arrays.asList(newVersion, newVersion2));
    assertNumChunks(project, newVersion, 1);
    assertNumChunks(project, newVersion2, 1);
    this.loader.cleanOlderProjectVersion(project.getId(), newVersion2 + 1,
        Arrays.asList(newVersion));
    assertNumChunks(project, newVersion, 1);
    assertNumChunks(project, newVersion2, 0);
  }

  private void assertNumChunks(final Project project, final int version, final int expectedChunks) {
    final ProjectFileHandler fileHandler = this.loader
        .fetchProjectMetaData(project.getId(), version);
    Assert.assertEquals(expectedChunks, fileHandler.getNumChunks());
  }

  private void assertGetUploadedFileOfCleanedVersion(final int project, final int version) {
    final Throwable thrown = catchThrowable(() -> this.loader.getUploadedFile(project, version));
    assertThat(thrown).isInstanceOf(ProjectManagerException.class);
    assertThat(thrown).hasMessageStartingWith(String.format("Got numChunks=0 for version %s of "
        + "project %s - seems like this version has been cleaned up", version, project));
  }

  @Test
  public void testUploadFlowFile() throws Exception {
    final File testYamlFile = ExecutionsTestUtil.getFlowFile(BASIC_FLOW_YAML_DIR, BASIC_FLOW_FILE);
    this.loader.uploadFlowFile(PROJECT_ID, PROJECT_VERSION, testYamlFile, FLOW_VERSION);
    final File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    final File file = this.loader
        .getUploadedFlowFile(PROJECT_ID, PROJECT_VERSION, BASIC_FLOW_FILE, FLOW_VERSION, tempDir);
    assertThat(file.getName()).isEqualTo(BASIC_FLOW_FILE);
    assertThat(FileUtils.contentEquals(testYamlFile, file)).isTrue();
  }

  @Test
  public void testDuplicateUploadFlowFileException() throws Exception {
    final File testYamlFile = ExecutionsTestUtil.getFlowFile(BASIC_FLOW_YAML_DIR, BASIC_FLOW_FILE);
    this.loader.uploadFlowFile(PROJECT_ID, PROJECT_VERSION, testYamlFile, FLOW_VERSION);

    assertThatThrownBy(
        () -> this.loader.uploadFlowFile(PROJECT_ID, PROJECT_VERSION, testYamlFile, FLOW_VERSION))
        .isInstanceOf(ProjectManagerException.class)
        .hasMessageContaining(
            "Error uploading flow file " + BASIC_FLOW_FILE + ", version " + FLOW_VERSION + ".");
  }

  @Test
  public void testUploadLargeFlowFileException() throws Exception {
    final File testYamlFile = ExecutionsTestUtil.getFlowFile(LARGE_FLOW_YAML_DIR, LARGE_FLOW_FILE);

    assertThatThrownBy(
        () -> this.loader.uploadFlowFile(PROJECT_ID, PROJECT_VERSION, testYamlFile, FLOW_VERSION))
        .isInstanceOf(ProjectManagerException.class)
        .hasMessageContaining(
            "Flow file length exceeds 10 MB limit.");
  }

  @Test
  public void testGetLatestFlowVersion() throws Exception {
    final File testYamlFile = ExecutionsTestUtil.getFlowFile(BASIC_FLOW_YAML_DIR, BASIC_FLOW_FILE);

    assertThat(
        this.loader.getLatestFlowVersion(PROJECT_ID, PROJECT_VERSION, testYamlFile.getName()))
        .isEqualTo(0);
    this.loader.uploadFlowFile(PROJECT_ID, PROJECT_VERSION, testYamlFile, FLOW_VERSION);
    assertThat(
        this.loader.getLatestFlowVersion(PROJECT_ID, PROJECT_VERSION, testYamlFile.getName()))
        .isEqualTo(FLOW_VERSION);
  }

  @After
  public void clearDB() {
    try {
      dbOperator.update("TRUNCATE TABLE project_flow_required_tags");
      dbOperator.update("TRUNCATE TABLE projects");
      dbOperator.update("TRUNCATE TABLE project_versions");
      dbOperator.update("TRUNCATE TABLE project_properties");
      dbOperator.update("TRUNCATE TABLE project_permissions");
      // can't use TRUNCATE due to foreign key constraints
      dbOperator.update("DELETE FROM project_flows");
      dbOperator.update("TRUNCATE TABLE project_files");
      dbOperator.update("TRUNCATE TABLE project_events");
      dbOperator.update("TRUNCATE TABLE project_flow_files");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }
}
