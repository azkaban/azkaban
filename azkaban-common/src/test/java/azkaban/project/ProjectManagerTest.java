package azkaban.project;

import static org.mockito.Mockito.anyCollection;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.storage.StorageManager;
import azkaban.user.User;
import azkaban.utils.Props;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ProjectManagerTest {

  private static final String PROJECT_NAME = "myTest";
  private static final String PROJECT_NAME_2 = "myTest_2";
  private static final String PROJECT_DESCRIPTION = "This is to test project manager";
  private static final String TEST_USER = "testUser";
  private static final String FILE_TYPE = "zip";
  private static final int PROJECT_ID = 1;
  private static final int PROJECT_ID_2 = 2;
  private static final int PROJECT_VERSION = 5;
  private static final int PROJECT_VERSION_RETENTIION = 3;
  private ProjectManager manager;
  private ProjectLoader loader;
  private StorageManager storageManager;
  private User user;

  @Before
  public void setUp() throws Exception {
    final Props props = new Props();
    this.loader = mock(ProjectLoader.class);
    this.storageManager = mock(StorageManager.class);
    this.manager = new ProjectManager(this.loader, this.storageManager, props);
    this.user = new User(TEST_USER);
    final Project project1 = new Project(PROJECT_ID, PROJECT_NAME);
    project1.setDescription(PROJECT_DESCRIPTION);
    project1.setActive(true);
    project1.setVersion(PROJECT_VERSION);

    when(this.loader.createNewProject(PROJECT_NAME, PROJECT_DESCRIPTION, this.user))
        .thenReturn(project1);
    when(this.loader.fetchProjectById(PROJECT_ID)).thenReturn(project1);
    when(this.loader.fetchProjectByName(PROJECT_NAME)).thenReturn(project1);
    when(this.loader.fetchAllProjectFlows(project1)).thenReturn(new ArrayList<>());
    when(this.loader.getLatestProjectVersion(project1)).thenReturn(PROJECT_VERSION);

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(final InvocationOnMock invocation) {
        project1.setActive(false);
        return null;
      }
    }).when(this.loader).removeProject(project1, this.user.getUserId());

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(final InvocationOnMock invocation) {
        project1.setVersion(PROJECT_VERSION + 1);
        return null;
      }
    }).when(this.loader).changeProjectVersion(project1, PROJECT_VERSION + 1, this.user.getUserId());

    doThrow(ProjectManagerException.class).when(this.loader).fetchAllProjectFlows(null);

  }

  @Test
  public void testCreateProject() throws Exception {
    System.out.println("TestCreateProject");
    final Project project = this.manager
        .createProject(PROJECT_NAME, PROJECT_DESCRIPTION, this.user);
    verify(this.loader)
        .postEvent(project, ProjectLogEvent.EventType.CREATED, this.user.getUserId(), null);
    Assert.assertEquals("Project Id", PROJECT_ID, project.getId());
    Assert.assertEquals("Project name", PROJECT_NAME, project.getName());
    Assert.assertEquals("Project description", PROJECT_DESCRIPTION,
        project.getDescription());
    Assert.assertTrue("Project is active", project.isActive());
  }

  @Test(expected = ProjectManagerException.class)
  public void testCreateProjectWithEmptyName() throws Exception {
    System.out.println("TestCreateProjectWithEmptyName");
    this.manager.createProject(null, PROJECT_DESCRIPTION, this.user);
  }

  @Test(expected = ProjectManagerException.class)
  public void testCreateProjectWithInvalidName() throws Exception {
    System.out.println("TestCreateProjectWithInvalidName");
    //Project name must start with a letter, test invalid project name "123", should throw exception
    this.manager.createProject("123", PROJECT_DESCRIPTION, this.user);
  }

  @Test(expected = ProjectManagerException.class)
  public void testCreateProjectWithEmptyDescription() throws Exception {
    System.out.println("testCreateProjectWithEmptyDescription");
    this.manager.createProject(PROJECT_NAME, null, this.user);
  }

  @Test(expected = ProjectManagerException.class)
  public void testCreateProjectWithEmptyUser() throws Exception {
    System.out.println("testCreateProjectWithEmptyUser");
    this.manager.createProject(PROJECT_NAME, PROJECT_DESCRIPTION, null);
  }

  @Test
  public void testRemoveProject() throws Exception {
    System.out.println("TestRemoveProject");
    final Project project = this.manager
        .createProject(PROJECT_NAME, PROJECT_DESCRIPTION, this.user);
    this.manager.removeProject(project, this.user);
    verify(this.loader).removeProject(project, this.user.getUserId());
    verify(this.loader).postEvent(project, ProjectLogEvent.EventType.DELETED, this.user.getUserId(),
        null);
    final Project fetchedProject = this.manager.getProject(project.getId());
    verify(this.loader).fetchProjectById(project.getId());
    verify(this.loader).fetchAllProjectFlows(project);
    Assert.assertFalse(fetchedProject.isActive());
  }

  @Test
  public void testUploadProject() throws Exception {
    System.out.println("TestUploadProject");
    final Project project = this.manager
        .createProject(PROJECT_NAME, PROJECT_DESCRIPTION, this.user);
    final File testFile = new File(
        this.getClass().getClassLoader().getResource("project/testjob/testjob.zip").getFile());
    System.out.println("Uploading zip file: " + testFile.getAbsolutePath());
    final Props props = new Props();
    this.manager.uploadProject(project, testFile, FILE_TYPE, this.user, props);

    verify(this.storageManager).uploadProject(project, PROJECT_VERSION + 1, testFile, this.user);

    verify(this.loader).uploadFlows(eq(project), eq(PROJECT_VERSION + 1), anyCollection());
    verify(this.loader).changeProjectVersion(project, PROJECT_VERSION + 1, this.user.getUserId());
    //uploadProjectProperties should be called twice, one for jobProps, the other for propProps
    verify(this.loader, times(2)).uploadProjectProperties(eq(project), anyList());
    verify(this.loader)
        .postEvent(project, ProjectLogEvent.EventType.UPLOADED, this.user.getUserId(),
            "Uploaded project files zip " + testFile.getName());
    verify(this.loader).cleanOlderProjectVersion(project.getId(),
        PROJECT_VERSION + 1 - PROJECT_VERSION_RETENTIION);
  }

  @Test
  public void testFetchProjectByName() throws Exception {
    System.out.println("TestFetchProjectByName");
    final Project project = this.manager
        .createProject(PROJECT_NAME, PROJECT_DESCRIPTION, this.user);
    final Project fetchedProject = this.manager.getProject(project.getName());
    Assert.assertEquals("Fetched project by name", project, fetchedProject);
  }

  @Test(expected = RuntimeException.class)
  public void testFetchInvalidProjectByName() throws Exception {
    System.out.println("TestFetchInvalidProjectByName");
    this.manager.createProject(PROJECT_NAME, PROJECT_DESCRIPTION, this.user);
    this.manager.getProject("Invalid_Project");
  }

  @Test
  public void testFetchProjectById() throws Exception {
    System.out.println("TestFetchProjectById");
    final Project project = this.manager
        .createProject(PROJECT_NAME, PROJECT_DESCRIPTION, this.user);
    final Project fetchedProject = this.manager.getProject(project.getId());
    Assert.assertEquals("Fetched project by id", project, fetchedProject);
  }

  @Test(expected = RuntimeException.class)
  public void testFetchInvalidProjectById() throws Exception {
    System.out.println("TestFetchInvalidProjectById");
    this.manager.createProject(PROJECT_NAME, PROJECT_DESCRIPTION, this.user);
    this.manager.getProject(100);
  }

  @Test
  public void testFetchAllProjects() throws Exception {
    System.out.println("TestFetchAllProjects");
    final List<Project> projects = new ArrayList<>();
    final Project new_project1 = this.manager.createProject(PROJECT_NAME, PROJECT_DESCRIPTION,
        this.user);
    final Project project2 = new Project(PROJECT_ID_2, PROJECT_NAME_2);
    project2.setDescription(PROJECT_DESCRIPTION);
    project2.setActive(true);
    project2.setVersion(PROJECT_VERSION);
    when(this.loader.createNewProject(PROJECT_NAME_2, PROJECT_DESCRIPTION, this.user))
        .thenReturn(project2);
    final Project new_project2 = this.manager
        .createProject(PROJECT_NAME_2, PROJECT_DESCRIPTION, this.user);
    projects.add(new_project1);
    projects.add(new_project2);

    when(this.loader.fetchAllActiveProjects()).thenReturn(projects);
    final List<Project> fetchedProjects = this.manager.getProjects();
    Assert.assertEquals("Fetched projects: ", projects, fetchedProjects);
  }
}
