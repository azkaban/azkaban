package azkaban.spi;

import azkaban.test.executions.ThinArchiveTestUtils;
import azkaban.utils.InvalidHashException;
import java.io.File;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;


public class DependencyTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  @Test
  public void testCreateValidDependency() throws Exception {
    Dependency depA = ThinArchiveTestUtils.getDepA();
    Dependency newDep =
        new Dependency(depA.getFileName(), depA.getDestination(), depA.getType(), depA.getIvyCoordinates(), depA.getSHA1());

    assertEquals(depA, newDep);
  }

  @Test
  public void testCopyDependency() throws Exception {
    Dependency depA = ThinArchiveTestUtils.getDepA();
    Dependency newDep = depA.makeCopy();

    assertEquals(depA, newDep);
  }

  @Test
  public void testMakeDependencyFile() throws Exception {
    Dependency depA = ThinArchiveTestUtils.getDepA();
    File file = TEMP_DIR.newFile(depA.getFileName());
    DependencyFile depFile = depA.makeDependencyFile(file);

    assertEquals(depA, depFile.makeCopy());
    assertEquals(depFile.getFile(), file);
  }

  @Test(expected = InvalidHashException.class)
  public void testInvalidHash() throws Exception {
    Dependency depA = ThinArchiveTestUtils.getDepA();
    new Dependency(
            depA.getFileName(),
            depA.getDestination(),
            depA.getType(),
            depA.getIvyCoordinates(),
            "uh oh, I'm not a hash :(");
  }
}
