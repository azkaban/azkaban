package azkaban.project;

import azkaban.storage.StorageManager;
import azkaban.user.User;
import azkaban.utils.Props;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ProjectManagerTest {
  private ProjectManager manager;
  private ProjectLoader loader;
  private StorageManager storageManager;
  private User user;
  private static final String PROJECT_NAME = "myTest";
  private static final String PROJECT_NAME_2 = "myTest_2";
  private static final String PROJECT_DESCRIPTION = "This is to test project manager";
  private static final String TEST_USER = "testUser";
  private static final String FILE_TYPE = "zip";
  private static final int PROJECT_ID = 1;
  private static final int PROJECT_ID_2 = 2;
  private static final int PROJECT_VERSION = 5;
  private static final int PROJECT_VERSION_RETENTIION = 3;

  @Before
  public void setUp() throws Exception {
    Props props = new Props();
    loader = mock(ProjectLoader.class);
    storageManager = mock(StorageManager.class);
    manager = new ProjectManager(loader, storageManager, props);
    user = new User(TEST_USER);
    Project project1 = new Project(PROJECT_ID, PROJECT_NAME);
    project1.setDescription(PROJECT_DESCRIPTION);
    project1.setActive(true);
    project1.setVersion(PROJECT_VERSION);

    when(loader.createNewProject(PROJECT_NAME, PROJECT_DESCRIPTION, user)).thenReturn(project1);
    when(loader.fetchProjectById(PROJECT_ID)).thenReturn(project1);
    when(loader.fetchProjectByName(PROJECT_NAME)).thenReturn(project1);
    when(loader.fetchAllProjectFlows(project1)).thenReturn(new ArrayList<>());
    when(loader.getLatestProjectVersion(project1)).thenReturn(PROJECT_VERSION);

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        project1.setActive(false);
        return null;
      }
    }).when(loader).removeProject(project1, user.getUserId());

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        project1.setVersion(PROJECT_VERSION + 1);
        return null;
      }
    }).when(loader).changeProjectVersion(project1, PROJECT_VERSION + 1, user.getUserId());

    doThrow(ProjectManagerException.class).when(loader).fetchAllProjectFlows(null);

  }

  @Test
  public void testCreateProject() throws Exception {
    System.out.println("TestCreateProject");
    Project project = manager.createProject(PROJECT_NAME, PROJECT_DESCRIPTION, user);
    verify(loader).postEvent(project, ProjectLogEvent.EventType.CREATED, user.getUserId(), null);
    Assert.assertEquals("Project Id", PROJECT_ID, project.getId());
    Assert.assertEquals("Project name", PROJECT_NAME, project.getName());
    Assert.assertEquals("Project description", PROJECT_DESCRIPTION,
        project.getDescription());
    Assert.assertTrue("Project is active", project.isActive());
  }

  @Test(expected = ProjectManagerException.class)
  public void testCreateProjectWithEmptyName() throws Exception {
    System.out.println("TestCreateProjectWithEmptyName");
    manager.createProject(null, PROJECT_DESCRIPTION, user);
  }

  @Test(expected = ProjectManagerException.class)
  public void testCreateProjectWithInvalidName() throws Exception {
    System.out.println("TestCreateProjectWithInvalidName");
    //Project name must start with a letter, test invalid project name "123", should throw exception
    manager.createProject("123", PROJECT_DESCRIPTION, user);
  }

  @Test(expected = ProjectManagerException.class)
  public void testCreateProjectWithEmptyDescription() throws Exception {
    System.out.println("testCreateProjectWithEmptyDescription");
    manager.createProject(PROJECT_NAME, null, user);
  }

  @Test(expected = ProjectManagerException.class)
  public void testCreateProjectWithEmptyUser() throws Exception {
    System.out.println("testCreateProjectWithEmptyUser");
    manager.createProject(PROJECT_NAME, PROJECT_DESCRIPTION, null);
  }

  @Test
  public void testRemoveProject() throws Exception {
    System.out.println("TestRemoveProject");
    Project project = manager.createProject(PROJECT_NAME, PROJECT_DESCRIPTION, user);
    manager.removeProject(project, user);
    verify(loader).removeProject(project, user.getUserId());
    verify(loader).postEvent(project, ProjectLogEvent.EventType.DELETED, user.getUserId(),
        null);
    Project fetchedProject = manager.getProject(project.getId());
    verify(loader).fetchProjectById(project.getId());
    verify(loader).fetchAllProjectFlows(project);
    Assert.assertFalse(fetchedProject.isActive());
  }

  @Test
  public void testUploadProject() throws Exception {
    System.out.println("TestUploadProject");
    Project project = manager.createProject(PROJECT_NAME, PROJECT_DESCRIPTION, user);
    File testFile = new File(this.getClass().getClassLoader().getResource("project/testjob/testjob.zip").getFile());
    System.out.println("Uploading zip file: " + testFile.getAbsolutePath());
    Props props = new Props();
    manager.uploadProject(project, testFile, FILE_TYPE, user, props);

    verify(storageManager).uploadProject(project, PROJECT_VERSION + 1, testFile, user);

    verify(loader).uploadFlows(eq(project), eq(PROJECT_VERSION + 1), anyCollection());
    verify(loader).changeProjectVersion(project, PROJECT_VERSION + 1, user.getUserId());
    //uploadProjectProperties should be called twice, one for jobProps, the other for propProps
    verify(loader, times(2)).uploadProjectProperties(eq(project), anyList());
    verify(loader).postEvent(project, ProjectLogEvent.EventType.UPLOADED, user.getUserId(),
        "Uploaded project files zip " + testFile.getName());
    verify(loader).cleanOlderProjectVersion(project.getId(), PROJECT_VERSION + 1 - PROJECT_VERSION_RETENTIION);
  }

  @Test
  public void testFetchProjectByName() throws Exception {
    System.out.println("TestFetchProjectByName");
    Project project = manager.createProject(PROJECT_NAME, PROJECT_DESCRIPTION, user);
    Project fetchedProject = manager.getProject(project.getName());
    Assert.assertEquals("Fetched project by name", project, fetchedProject);
  }

  @Test(expected = RuntimeException.class)
  public void testFetchInvalidProjectByName() throws Exception {
    System.out.println("TestFetchInvalidProjectByName");
    manager.createProject(PROJECT_NAME, PROJECT_DESCRIPTION, user);
    manager.getProject("Invalid_Project");
  }

  @Test
  public void testFetchProjectById() throws Exception {
    System.out.println("TestFetchProjectById");
    Project project = manager.createProject(PROJECT_NAME, PROJECT_DESCRIPTION, user);
    Project fetchedProject = manager.getProject(project.getId());
    Assert.assertEquals("Fetched project by id", project, fetchedProject);
  }

  @Test(expected = RuntimeException.class)
  public void testFetchInvalidProjectById() throws Exception {
    System.out.println("TestFetchInvalidProjectById");
    manager.createProject(PROJECT_NAME, PROJECT_DESCRIPTION, user);
    manager.getProject(100);
  }

  @Test
  public void testFetchAllProjects() throws Exception {
    System.out.println("TestFetchAllProjects");
    List<Project> projects = new ArrayList<>();
    Project new_project1 = manager.createProject(PROJECT_NAME, PROJECT_DESCRIPTION, user);
    Project project2 = new Project(PROJECT_ID_2, PROJECT_NAME_2);
    project2.setDescription(PROJECT_DESCRIPTION);
    project2.setActive(true);
    project2.setVersion(PROJECT_VERSION);
    when(loader.createNewProject(PROJECT_NAME_2, PROJECT_DESCRIPTION, user)).thenReturn(project2);
    Project new_project2 = manager.createProject(PROJECT_NAME_2, PROJECT_DESCRIPTION, user);
    projects.add(new_project1);
    projects.add(new_project2);

    when(loader.fetchAllActiveProjects()).thenReturn(projects);
    List<Project> fetchedProjects = manager.getProjects();
    Assert.assertEquals("Fetched projects: ", projects, fetchedProjects);
  }
}
