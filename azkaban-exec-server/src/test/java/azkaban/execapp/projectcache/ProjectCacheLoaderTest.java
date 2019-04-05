package azkaban.execapp.projectcache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutorManagerException;
import azkaban.project.ProjectFileHandler;
import azkaban.storage.StorageManager;
import azkaban.utils.FileIOUtils;
import java.io.File;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ProjectCacheLoaderTest {
  public static final String SAMPLE_FLOW_01 = "sample_flow_01";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File projectsDir;
  private ProjectCacheKey projectKey = new ProjectCacheKey(12, 34);
  private final long actualDirSize = 1048835;

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
    ProjectCacheLoader loader = new ProjectCacheLoader(createMockStorageManager(), this
        .projectsDir);
    File tmpDir = new File(projectsDir, "testDownloadProjectFile");
    assertThatThrownBy(() -> loader.downloadProject(new ProjectCacheKey(35, 43), tmpDir))
        .hasCauseInstanceOf(ExecutorManagerException.class);
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
/*
  @Test
  public void testSetupFlowByMultipleThreads() {
    final int threadNum = 4;

    final ExecutableFlow[] executableFlows = new ExecutableFlow[]{
        mockExecutableFlow(1, 12, 34),
        mockExecutableFlow(2, 12, 34),
        mockExecutableFlow(3, 12, 34),
        mockExecutableFlow(4, 12, 34)
    };

    final ExecutorService service = Executors.newFixedThreadPool(threadNum);

    final List<Future> futures = new ArrayList<>();
    for (int i = 0; i < threadNum; i++) {
      final int finalI = i;
      futures.add(service.submit(() -> {
        assertThatCode(() -> this.instance.setup(executableFlows[finalI])
        ).doesNotThrowAnyException();
      }));
    }

    for (final Future future : futures) {
      assertThatCode(() -> future.get()).doesNotThrowAnyException();
    }

    service.shutdownNow();
    for (final ExecutableFlow flow : executableFlows) {
      final File execDir = new File(this.executionsDir, String.valueOf(flow.getExecutionId()));
      assertTrue(execDir.exists());
      assertTrue(new File(execDir, SAMPLE_FLOW_01).exists());
    }
  }

  @Test
  public void testSetupFlow() throws ExecutorManagerException {
    final ExecutableFlow executableFlow = mock(ExecutableFlow.class);
    when(executableFlow.getExecutionId()).thenReturn(12345);
    when(executableFlow.getProjectId()).thenReturn(12);
    when(executableFlow.getVersion()).thenReturn(34);

    this.instance.setup(executableFlow);
    final File execDir = new File(this.executionsDir, "12345");
    assertTrue(execDir.exists());
    assertTrue(new File(execDir, SAMPLE_FLOW_01).exists());
  }


  @Test
  public void testProjectsCacheMetricsZeroHit() {
    //given
    final ProjectCacheMetrics cacheMetrics = new ProjectCacheMetrics();

    //when zero hit and zero miss then
    assertThat(cacheMetrics.getHitRatio()).isEqualTo(0);

    //when
    cacheMetrics.incrementCacheMiss();
    //then
    assertThat(cacheMetrics.getHitRatio()).isEqualTo(0);
  }

  @Test
  public void testProjectsCacheMetricsHit() {
    //given
    final ProjectCacheMetrics cacheMetrics = new ProjectCacheMetrics();

    //when one hit
    cacheMetrics.incrementCacheHit();
    //then
    assertThat(cacheMetrics.getHitRatio()).isEqualTo(1);

    //when one miss
    cacheMetrics.incrementCacheMiss();
    //then
    assertThat(cacheMetrics.getHitRatio()).isEqualTo(0.5);
  }
*/

}
