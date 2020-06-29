package azkaban.storage;

import azkaban.AzkabanCommonModuleConfig;
import azkaban.spi.Dependency;
import azkaban.test.executions.ThinArchiveTestUtils;
import java.io.File;
import java.net.URI;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class LocalHadoopStorageTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private static final String DEP_ROOT_URI = "http://www.someplace.com/path/to/dep/";
  private File BASE_DIRECTORY;

  private Dependency depA;
  private Path expectedPathDepA;

  private LocalHadoopStorage localStorage;
  private FileSystem http;
  private AzkabanCommonModuleConfig config;

  @Before
  public void setUp() throws Exception {
    BASE_DIRECTORY = TEMP_DIR.newFolder("TEST_LOCAL_STORAGE");

    this.http = mock(FileSystem.class);
    this.config = mock(AzkabanCommonModuleConfig.class);
    when(this.config.getLocalStorageBaseDirPath()).thenReturn(BASE_DIRECTORY.getCanonicalPath());
    when(this.config.getOriginDependencyRootUri()).thenReturn(URI.create(DEP_ROOT_URI));

    this.localStorage = new LocalHadoopStorage(config, http);

    this.depA = ThinArchiveTestUtils.getDepA();
    this.expectedPathDepA = new Path(DEP_ROOT_URI + ThinArchiveTestUtils.getDepAPath());
  }

  @Test
  public void testSetUpWithoutHttpFileSystem() throws Exception {
    // Ensure we can still instantiate LocalHadoopStorage with the HTTP FileSystem being null.
    // Fetching dependencies will not be supported.
    this.localStorage = new LocalHadoopStorage(this.config, null);
    assertFalse(this.localStorage.dependencyFetchingEnabled());

    boolean hitException = false;
    try {
      this.localStorage.getDependency(depA);
    } catch (UnsupportedOperationException e) {
      hitException = true;
    }
    if (!hitException) fail("Expected UnsupportedOperationException when fetching dependency.");
  }

  @Test
  public void testGetDependency() throws Exception {
    FSDataInputStream sampleInputStream = mock(FSDataInputStream.class);
    doReturn(sampleInputStream).when(this.http).open(expectedPathDepA);

    assertEquals(sampleInputStream, this.localStorage.getDependency(depA));
  }
}
