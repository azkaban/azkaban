package azkaban.project;

import azkaban.spi.Dependency;
import azkaban.test.executions.ThinArchiveTestUtils;
import azkaban.utils.JSONUtils;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static azkaban.test.executions.ThinArchiveTestUtils.*;
import static azkaban.utils.ThinArchiveUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ThinArchiveUtilsTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();
  public File startupDependenciesFolder;
  private File startupDepsJsonFile;
  private Set<Dependency> depSetABCAndNull;

  @Before
  public void setUp() throws Exception {
    depSetABCAndNull = ThinArchiveTestUtils.getDepSetABCAndNull();
    startupDependenciesFolder = TEMP_DIR.newFolder("TestStartupDepsFolder");
    startupDepsJsonFile = getStartupDependenciesFile(startupDependenciesFolder);
  }

  /**
   * Test parsing of startup dependencies functionality. Even if json containing startup
   * dependencies for a project contains a 'null' in the list, parseStartupDependencies
   * should check for it and remove it from the final set of dependencies.
   *
   * @throws Exception
   */
  @Test
  public void testParseStartupDependencies() throws Exception {
    Set<Dependency> finalDependencies = new HashSet<>();
    try {
      // Get a raw json which contains null along with other valid dependencies
      String rawJson = getRawJSONDepsNullAndABC();
      finalDependencies = parseStartupDependencies(rawJson);
    } catch (NullPointerException npe) {
      // If NullPointerException is thrown by parseStartupDependencies() it means the
      // method tried to create a Dependency object without checking for null.
      // Fail the test since it should always check for null before creating Dependency
      // object.
      fail("Received NullPointerException while parsing startup dependencies");
    }
    // The Set of dependencies returned by parseStartupDependencies() should be without null
    // and same as Set of dependencies created by the test for valid dependencies in the
    // raw json.
    assertEquals(finalDependencies, getDepSetABC());
  }

  /**
   * Test writing of startup dependencies to file.
   *
   * When Set of dependencies containing 'null' along with valid Dependency objects is
   * passed to writeStartupDependencies(), the method should filter out null before
   * writing the dependencies to the file.
   *
   * After writing to file, the test reads the file to check if any null is present.
   *
   * @throws Exception
   */
  @Test
  public void testWriteStartupDependencies() throws Exception {
    // Pass Set containing null and valid dependencies to writeStartupDependencies()
    writeStartupDependencies(startupDepsJsonFile, depSetABCAndNull);

    // Read the file that was written above to check if contains null
    String rawJson = FileUtils.readFileToString(startupDepsJsonFile);
    List<Map<String, String>> rawParseResult =
        ((HashMap<String, List<Map<String, String>>>) JSONUtils.parseJSONFromString(rawJson))
            .get("dependencies");

    if (rawParseResult == null) {
      // Fail the test if the file does not contain "dependencies" key
      fail("Could not find 'dependencies' key in startup-dependencies.json file.");
    }

    Set<Dependency> dependenciesFromFile = parseStartupDependencies(startupDepsJsonFile);
    for (Map<String, String> rawDependency : rawParseResult) {
      if (rawDependency != null) {
        dependenciesFromFile.add(new Dependency(rawDependency));
      } else {
        // Fail the test if file contains a null
        fail("Found a null entry in startup-dependencies.json file.");
      }
    }

    // The Set of dependencies returned by parseStartupDependencies() should be without null
    // and same as Set of dependencies created by the test for valid dependencies.
    assertEquals(dependenciesFromFile, getDepSetABC());
  }

}
