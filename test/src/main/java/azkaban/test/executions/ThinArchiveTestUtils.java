package azkaban.test.executions;

import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.mockito.ArgumentMatcher;

import static org.mockito.ArgumentMatchers.*;

// Custom mockito argument matcher to help with matching Dependency with DependencyFile
class DependencyMatcher implements ArgumentMatcher<DependencyFile> {
  private Dependency dep;
  public DependencyMatcher(final Dependency d) {
    this.dep = d;
  }

  @Override
  public boolean matches(final DependencyFile depFile) {
    try {
      return dep.getFileName().equals(((Dependency) depFile).getFileName());
    } catch (Exception e) {
      return false;
    }
  }
}

// Custom mockito argument matcher to help with matching Set<Dependency> with Set<DependencyFile>
class DependencySetMatcher implements ArgumentMatcher<Set<DependencyFile>> {
  private Set<Dependency> deps;
  public DependencySetMatcher(final Set<Dependency> deps) {
    this.deps = deps;
  }

  @Override
  public boolean matches(final Set<DependencyFile> depFiles) {
    try {
      Set<String> depsFileNames = deps.stream().map(Dependency::getFileName).collect(Collectors.toSet());
      Set<String> depFilesFileNames = depFiles.stream().map(Dependency::getFileName).collect(Collectors.toSet());
      return depsFileNames.equals(depFilesFileNames);
    } catch (Exception e) {
      return false;
    }
  }
}


public class ThinArchiveTestUtils {
  public static DependencyFile depEq(final Dependency dep) {
    return argThat(new DependencyMatcher(dep));
  }
  public static Set<DependencyFile> depSetEq(final Set<Dependency> deps) {
    return argThat(new DependencySetMatcher(deps));
  }

  public static void makeSampleThinProjectDirAB(final File projectFolder) throws IOException {
    // Create test project directory
    // ../
    // ../lib/some-snapshot.jar
    // ../app-meta/startup-dependencies.json
    File libFolder = new File(projectFolder, "lib");
    libFolder.mkdirs();
    File appMetaFolder = new File(projectFolder, "app-meta");
    libFolder.mkdirs();
    FileUtils.writeStringToFile(new File(libFolder, "some-snapshot.jar"), "oldcontent");
    FileUtils.writeStringToFile(new File(appMetaFolder, "startup-dependencies.json"),
        ThinArchiveTestUtils.getRawJSONDepsAB());
  }

  public static Set getDepSetA() { return new HashSet(Arrays.asList(getDepA())); }
  public static Set getDepSetB() { return new HashSet(Arrays.asList(getDepB())); }
  public static Set getDepSetAB() { return new HashSet(Arrays.asList(getDepA(), getDepB())); }
  public static Set getDepSetABC() { return new HashSet(Arrays.asList(getDepA(), getDepB(), getDepC())); }

  public static String getRawJSONDepA() {
    return "{" +
        "    \"dependencies\": [" +
                depAJSONBlock() +
        "    ]" +
        "}";
  }

  public static String getRawJSONDepB() {
    return "{" +
        "    \"dependencies\": [" +
                depBJSONBlock() +
        "    ]" +
        "}";
  }

  public static String getRawJSONDepC() {
    return "{" +
        "    \"dependencies\": [" +
                depCJSONBlock() +
        "    ]" +
        "}";
  }

  public static String getRawJSONDepsAB() {
    return "{" +
        "    \"dependencies\": [" +
                depAJSONBlock() + "," +
                depBJSONBlock() +
        "    ]" +
        "}";
  }

  public static String getRawJSONDepsABC() {
    return "{" +
        "    \"dependencies\": [" +
                depAJSONBlock() + "," +
                depBJSONBlock() + "," +
                depCJSONBlock() +
        "    ]" +
        "}";
  }

  private static String depAJSONBlock() {
    return "{" +
        "    \"sha1\": \"131bd316a77423e6b80d93262b576c139c72b4c3\"," +
        "    \"file\": \"aaaa.jar\"," +
        "    \"destination\": \"lib\"," +
        "    \"type\": \"jar\"," +
        "    \"ivyCoordinates\": \"com.linkedin.test:testeraaaa:1.0.1\"" +
        "}";
  }

  private static String depBJSONBlock() {
    return "{" +
        "    \"sha1\": \"9461919846e1e7c8fc74fee95aa6ac74993be71e\"," +
        "    \"file\": \"bbbb.jar\"," +
        "    \"destination\": \"dep_b_folder\"," +
        "    \"type\": \"jar\"," +
        "    \"ivyCoordinates\": \"com.linkedin.test:testerbbbb:1.0.1\"" +
        "}";
  }

  private static String depCJSONBlock() {
    return "{" +
        "    \"sha1\": \"f873f39163f5b43dbf1fee63cbce284074896221\"," +
        "    \"file\": \"cccc.jar\"," +
        "    \"destination\": \"dep_c_folder\"," +
        "    \"type\": \"jar\"," +
        "    \"ivyCoordinates\": \"com.linkedin.test:testercccc:1.0.1\"" +
        "}";
  }

  public static String getDepAContent() { return "blahblah12"; }
  public static String getDepAPath() {
    return "com/linkedin/test/testeraaaa/1.0.1/aaaa.jar";
  }
  public static Dependency getDepA() {
    try {
      return new Dependency(
          "aaaa.jar",
          "lib",
          "jar",
          "com.linkedin.test:testeraaaa:1.0.1",
          "131bd316a77423e6b80d93262b576c139c72b4c3");
    } catch (Exception e) {
      // This will never happen
      throw new RuntimeException("Test utils tried to create a dependency with an invalid hash.");
    }
  }

  public static String getDepBContent() { return "ladedah83"; }
  public static String getDepBPath() {
    return "com/linkedin/test/testerbbbb/1.0.1/bbbb.jar";
  }
  public static Dependency getDepB() {
    try {
      return new Dependency(
        "bbbb.jar",
        "dep_b_folder",
        "jar",
        "com.linkedin.test:testerbbbb:1.0.1",
        "9461919846e1e7c8fc74fee95aa6ac74993be71e");
    } catch (Exception e) {
      // This will never happen
      throw new RuntimeException("Test utils tried to create a dependency with an invalid hash.");
    }
  }

  public static String getDepCContent() { return "myprecious"; }
  public static String getDepCPath() {
    return "com/linkedin/test/testercccc/1.0.1/cccc.jar";
  }
  public static Dependency getDepC() {
    try {
      return new Dependency(
          "cccc.jar",
          "dep_c_folder",
          "jar",
          "com.linkedin.test:testercccc:1.0.1",
          "f873f39163f5b43dbf1fee63cbce284074896221");
    } catch (Exception e) {
      // This will never happen
      throw new RuntimeException("Test utils tried to create a dependency with an invalid hash.");
    }
  }
}
