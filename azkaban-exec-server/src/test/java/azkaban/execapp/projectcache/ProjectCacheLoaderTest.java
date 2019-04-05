package azkaban.execapp.projectcache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.project.ProjectFileHandler;
import azkaban.spi.StorageException;
import azkaban.storage.StorageManager;
import azkaban.utils.FileIOUtils;
import java.io.File;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test the {@link ProjectCacheLoader} */
public class ProjectCacheLoaderTest {
  public static final String SAMPLE_FLOW_01 = "sample_flow_01";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File projectsDir;
  private ProjectCacheKey projectKey = new ProjectCacheKey(12, 34);
  private final long actualDirSize = 1048835;

  /** Create a {@link azkaban.storage.StorageManager */
  private StorageManager createMockStorageManager() {
    final ClassLoader classLoader = getClass().getClassLoader();
    final File file = new File(classLoader.getResource(SAMPLE_FLOW_01 + ".zip").getFile());

    final ProjectFileHandler projectFileHandler = mock(ProjectFileHandler.class);
    when(projectFileHandler.getFileType()).thenReturn("zip");
    when(projectFileHandler.getLocalFile()).thenReturn(file);

    final StorageManager storageManager = mock(StorageManager.class);
    when(storageManager.getProjectFile(anyInt(), anyInt())).thenReturn(projectFileHandler);
    return storageManager;
  }

  /** Create a {@link azkaban.storage.StorageManager} that throws an exception. */
  private StorageManager createBadMockStorageManager() {
    final ClassLoader classLoader = getClass().getClassLoader();
    final File file = new File(classLoader.getResource(SAMPLE_FLOW_01 + ".zip").getFile());

    final ProjectFileHandler projectFileHandler = mock(ProjectFileHandler.class);
    when(projectFileHandler.getFileType()).thenReturn("zip");
    when(projectFileHandler.getLocalFile()).thenReturn(file);

    final StorageManager storageManager = mock(StorageManager.class);
    when(storageManager.getProjectFile(anyInt(), anyInt())).thenThrow(new
        StorageException("non existent project"));
    return storageManager;
  }


  @Before
  public void setUp() throws Exception {
    this.projectsDir = this.temporaryFolder.newFolder("projects");
  }

  @Test
  public void testDownloadProjectFile() throws Exception {
    ProjectCacheLoader loader = new ProjectCacheLoader(createMockStorageManager(), this
        .projectsDir);
    File tmpDir = new File(projectsDir, "testDownloadProjectFile");
    long size = loader.downloadProject(projectKey, tmpDir);

    // check project size is set properly
     assertThat(size).isEqualTo(actualDirSize);
    assertThat(FileIOUtils.readNumberFromFile(
        Paths.get(tmpDir.getPath(), ProjectCacheLoader.PROJECT_DIR_SIZE_FILE_NAME)))
        .isEqualTo(actualDirSize);

    assertThat(tmpDir).isNotNull();
    assertThat(tmpDir.list()).contains(SAMPLE_FLOW_01);
  }

  @Test
  public void testDownloadNonExistentProjectFile() {
    ProjectCacheLoader loader = new ProjectCacheLoader(createBadMockStorageManager(), this
        .projectsDir);
    File tmpDir = new File(projectsDir, "testDownloadProjectFile");
    assertThatThrownBy(() -> loader.downloadProject(new ProjectCacheKey(35, 43), tmpDir))
        .hasCauseInstanceOf(StorageException.class);
  }

  @Test
  public void testLoadRemove() throws Exception {
    ProjectCacheLoader loader = new ProjectCacheLoader(createMockStorageManager(), this
        .projectsDir);

    // load the project
    ProjectDirectoryInfo project = loader.load(projectKey);
    assertThat(project.getSizeInBytes()).isEqualTo(actualDirSize);
    assertThat(project.getDirectory()).isNotNull();
    assertThat(project.getDirectory().list()).contains(SAMPLE_FLOW_01);
    assertThat(project.getKey()).isEqualTo(projectKey);

    // remove the project
    loader.remove(projectKey, project);
    assertThat(project.getDirectory().exists()).isFalse();

  }
}
