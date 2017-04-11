package azkaban.project;

import azkaban.user.User;
import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ProjectManagerTest {

  private File workingDir;

  @Before
  public void setUp() throws Exception {
    System.out.println("projectManagerTest: create temp dir");
    synchronized (this) {
      workingDir = new File("_AzkabanTestDir_" + System.currentTimeMillis());
      if (workingDir.exists()) {
        FileUtils.deleteDirectory(workingDir);
      }
      workingDir.mkdirs();
    }
  }

  @After
  public void tearDown() throws IOException {
    System.out.println("Teardown temp dir");
    synchronized (this) {
      if (workingDir != null) {
        FileUtils.deleteDirectory(workingDir);
        workingDir = null;
      }
    }
  }

  @Test
  public void testCreateProject() throws ProjectManagerException {
    System.out.println("projectManagerTest: testCreateProject");
    ProjectManager manager = createProjectManager();
    String projectName = "myTestCreateProject";
    String projectDescription = "This is to test creating a project";
    User user = new User("testUser");
    createProjectUtil(manager, projectName, projectDescription, user);
    System.out.println("Created project successfully");
  }

  @Test
  public void testRemoveProject() throws ProjectManagerException {
    System.out.println("projectManagerTest: testRemoveProject");
    ProjectManager manager = createProjectManager();
    String projectName = "myTestRemoveProject";
    String projectDescription = "This is to test removing a project";
    User user = new User("testUser");
    Project project = createProjectUtil(manager, projectName, projectDescription, user);

    manager.removeProject(project, user);
    Project fetchedProject = manager.getProject(project.getId());
    Assert.assertFalse(fetchedProject.isActive());
    System.out.println("Removed project successfully");
  }

  @Test
  public void testUploadProject() throws ProjectManagerException {
    System.out.println("projectManagerTest: testUploadProject");
    ProjectManager manager = createProjectManager();
    String projectName = "myTestUploadProject";
    String projectDescription = "This is to test uploading a project";
    User user = new User("testUser");
    Project project = createProjectUtil(manager, projectName, projectDescription, user);

    File testDir = new File("src/test/resources/project/testjob/testjob.zip");
    System.out.println("Uploading zip file: " + testDir.getAbsolutePath());
    Props props = new Props();
    manager.uploadProject(project, testDir, "zip", user, props);
    System.out.println("Uploaded project successfully");
  }

  @Test
  public void testFetchProjectByName() throws ProjectManagerException {
    System.out.println("projectManagerTest: testFetchProjectByName");
    ProjectManager manager = createProjectManager();
    String projectName = "myTestFetchProjectByName";
    String projectDescription = "This is to test fetching a project by name";
    User user = new User("testUser");
    Project project = createProjectUtil(manager, projectName, projectDescription, user);

    Project fetchedProject = manager.getProject(project.getName());
    assertProjectMemberEquals(project, fetchedProject);
    System.out.println("Fetched project by name successfully");
  }

  @Test
  public void testFetchProjectById() throws ProjectManagerException {
    System.out.println("projectManagerTest: testFetchProjectById");
    ProjectManager manager = createProjectManager();
    String projectName = "myTestFetchProjectById";
    String projectDescription = "This is to test fetching a project by id";
    User user = new User("testUser");
    Project project = createProjectUtil(manager, projectName, projectDescription, user);

    Project fetchedProject = manager.getProject(project.getId());
    assertProjectMemberEquals(project, fetchedProject);
    System.out.println("Fetched project by id successfully");
  }

  @Test
  public void testFetchAllProjects() throws ProjectManagerException {
    System.out.println("projectManagerTest: testFetchAllProjects");
    ProjectManager manager = createProjectManager();
    String projectName1 = "myTestFetchAllProjects_1";
    String projectDescription1 = "This is to test fetching all projects: project_1";
    User user = new User("testUser");
    Project project1 = createProjectUtil(manager, projectName1, projectDescription1, user);

    String projectName2 = "myTestFetchAllProjects_2";
    String projectDescription2 = "This is to test fetching all projects: project_2";
    Project project2 = createProjectUtil(manager, projectName2, projectDescription2, user);

    List<Project> fetchedProjects = manager.getProjects();
    Assert.assertTrue("Fetched project_1", fetchedProjects.contains(project1));
    Assert.assertTrue("Fetched project_2", fetchedProjects.contains(project2));
    System.out.println("Fetched all projects successfully");
  }

  private ProjectManager createProjectManager() {
    Props props = new Props();
    ProjectLoader loader = new MockProjectLoader(workingDir);
    return new ProjectManager(loader, props);
  }

  private Project createProjectUtil(ProjectManager manager, String projectName, String description,
      User creator) throws ProjectManagerException {
    Project project = null;
    try {
      project = manager.createProject(projectName, description, creator);
    } catch (ProjectManagerException e) {
      System.out.println("Creating project failed");
      Assert.fail(e.getMessage());
    }

    Assert.assertTrue("Project Id set", project.getId() > -1);
    Assert.assertEquals("Project name", projectName, project.getName());
    Assert.assertEquals("Project description", description,
        project.getDescription());
    Assert.assertTrue("Project is active", project.isActive());
    return project;
  }

  private void assertProjectMemberEquals(Project p1, Project p2) {
    Assert.assertEquals(p1.getId(), p2.getId());
    Assert.assertEquals(p1.getName(), p2.getName());
    Assert.assertEquals(p1.getDescription(), p2.getDescription());
    Assert.assertEquals(p1.isActive(), p2.isActive());
    System.out.println("Project " + p1.getName() + " equals to project " + p2.getName());
  }
}
