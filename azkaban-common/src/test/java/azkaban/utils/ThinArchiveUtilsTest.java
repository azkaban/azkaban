/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.utils;

import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import azkaban.test.executions.ThinArchiveTestUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.skyscreamer.jsonassert.JSONAssert;

import static azkaban.utils.ThinArchiveUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class ThinArchiveUtilsTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private static final String HDFS_DEP_ROOT_PATH = "hdfs://localhost:9000/some/cool/place/";

  private Logger log;

  @Before
  public void setup() {
    log = mock(Logger.class);
  }

  @Test
  public void testGetStartupDependenciesFile() throws Exception {
    File someFolder = TEMP_DIR.newFolder("someproject");
    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(someFolder);

    assertEquals(someFolder.getAbsolutePath() + "/app-meta/startup-dependencies.json", startupDependenciesFile.getAbsolutePath());
  }

  @Test
  public void testGetDependencyFile() throws Exception {
    File someFolder = TEMP_DIR.newFolder("someproject");

    DependencyFile dependencyFile = ThinArchiveUtils.getDependencyFile(someFolder, ThinArchiveTestUtils.getDepA());
    DependencyFile expectedDependencyFile = ThinArchiveTestUtils.getDepA().makeDependencyFile(
        new File(someFolder, ThinArchiveTestUtils.getDepA().getDestination()
        + File.separator
        + ThinArchiveTestUtils.getDepA().getFileName()));

    assertEquals(expectedDependencyFile, dependencyFile);
  }

  @Test
  public void testWriteStartupDependencies() throws Exception {
    File outFile = TEMP_DIR.newFile("startup-dependencies.json");
    ThinArchiveUtils.writeStartupDependencies(outFile, ThinArchiveTestUtils.getDepSetAB());

    String writtenJSON = FileUtils.readFileToString(outFile);
    JSONAssert.assertEquals(ThinArchiveTestUtils.getRawJSONDepsAB(), writtenJSON, false);
  }

  @Test
  public void testReadStartupDependencies() throws Exception {
    File inFile = TEMP_DIR.newFile("startup-dependencies.json");
    FileUtils.writeStringToFile(inFile, ThinArchiveTestUtils.getRawJSONDepsAB());

    Set<Dependency> parsedDependencies = ThinArchiveUtils.parseStartupDependencies(inFile);
    assertEquals(ThinArchiveTestUtils.getDepSetAB(), parsedDependencies);
  }

  @Test
  public void testReadEmptyStartupDependencies() throws Exception {
    // An empty startup-dependencies.json should be interpreted as an empty list of startup dependencies
    File inFile = TEMP_DIR.newFile("startup-dependencies.json");
    FileUtils.writeStringToFile(inFile, "");

    Set<Dependency> parsedDependencies = ThinArchiveUtils.parseStartupDependencies(inFile);
    assertEquals(Collections.emptySet(), parsedDependencies);
  }

  @Test(expected = IOException.class)
  public void testReadInvalidMissingKeyStartupDependencies() throws Exception {
    File inFile = TEMP_DIR.newFile("startup-dependencies.json");
    FileUtils.writeStringToFile(inFile, "{}");

    ThinArchiveUtils.parseStartupDependencies(inFile);
  }

  @Test(expected = IOException.class)
  public void testReadInvalidMalformedJsonStartupDependencies() throws Exception {
    File inFile = TEMP_DIR.newFile("startup-dependencies.json");
    FileUtils.writeStringToFile(inFile, "{]ugh!thisisn'tjson");

    ThinArchiveUtils.parseStartupDependencies(inFile);
  }

  @Test
  public void testConvertIvyCoordinateToPath() throws Exception {
    assertEquals(ThinArchiveTestUtils.getDepAPath(),
        ThinArchiveUtils.convertIvyCoordinateToPath(ThinArchiveTestUtils.getDepA()));
  }

  @Test
  public void testReplaceLocalPathsWithStoragePathsTHIN() throws Exception {
    // This is a test on a project from a THIN archive (and we expect the path for the one dependency listed
    // in startup-dependencies.json, depA) to be replaced with the HDFS path, but the other jar should still have
    // its normal local filepath

    Dependency depA = ThinArchiveTestUtils.getDepA();
    File projectDir = TEMP_DIR.newFolder("sample_proj");

    Props props = new Props();
    props.put(DEPENDENCY_STORAGE_ROOT_PATH_PROP, HDFS_DEP_ROOT_PATH);

    // Put depA in the correct location
    File depAFile = new File(projectDir, depA.getDestination() + File.separator + depA.getFileName());
    FileUtils.writeStringToFile(depAFile, ThinArchiveTestUtils.getDepAContent());

    // Put some other random jar in the same folder as depA
    File otherRandomJar = new File(projectDir, depA.getDestination() + File.separator + "blahblah-1.0.0.jar");
    FileUtils.writeStringToFile(otherRandomJar, "somerandomcontent");

    // Write the startup-dependencies.json file containing only depA
    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(projectDir);
    FileUtils.writeStringToFile(startupDependenciesFile, ThinArchiveTestUtils.getRawJSONDepA());

    List<String> jarPaths = new ArrayList<>();
    // Add both paths relative to root of project folder
    jarPaths.add("./" + depA.getDestination() + File.separator + depA.getFileName());
    jarPaths.add("./" + depA.getDestination() + File.separator + "blahblah-1.0.0.jar");
    String jarPathsStr = String.join(",", jarPaths);

    String resultingJarPathsStr =
        ThinArchiveUtils.replaceLocalPathsWithStoragePaths(projectDir, jarPathsStr, props, log);

    List<String> expectedJarPaths = new ArrayList<>();
    expectedJarPaths.add(HDFS_DEP_ROOT_PATH + ThinArchiveUtils.convertIvyCoordinateToPath(depA));
    expectedJarPaths.add(jarPaths.get(1));
    String expectedJarPathsStr = String.join(",", expectedJarPaths);

    assertEquals(expectedJarPathsStr, resultingJarPathsStr);
  }

  @Test
  public void testReplaceLocalPathsWithStoragePathsFAT() throws Exception {
    // This is a test on a project from a FAT archive (essentially because there will be no startup-depencencies.json
    // file in this project, we expect none of the paths to be replaced (the input paths should be returned without
    // any modification)

    Dependency depA = ThinArchiveTestUtils.getDepA();
    File projectDir = TEMP_DIR.newFolder("sample_proj");

    Props props = new Props();
    props.put(DEPENDENCY_STORAGE_ROOT_PATH_PROP, HDFS_DEP_ROOT_PATH);

    // Put depA in the correct location
    File depAFile = new File(projectDir, depA.getDestination() + File.separator + depA.getFileName());
    FileUtils.writeStringToFile(depAFile, ThinArchiveTestUtils.getDepAContent());

    List<String> jarPaths = new ArrayList<>();
    jarPaths.add(depAFile.getCanonicalPath());
    String jarPathsStr = String.join(",", jarPaths);

    List<String> expectedJarPaths = new ArrayList<>();
    expectedJarPaths.add(jarPaths.get(0));
    String expectedJarPathsStr = String.join(",", expectedJarPaths);

    String resultingJarPathsStr =
        ThinArchiveUtils.replaceLocalPathsWithStoragePaths(projectDir, jarPathsStr, props, log);

    assertEquals(expectedJarPathsStr, resultingJarPathsStr);
  }

  @Test
  public void testReplaceLocalPathsWithStoragePathsNoDependencyStorageRootPath() throws Exception {
    // This is a test on a THIN project, but we don't set the DEPENDENCY_STORAGE_ROOT_PATH_PROP so we
    // expect none of the paths to be replaced (the input paths should be returned without any modification)

    Dependency depA = ThinArchiveTestUtils.getDepA();
    File projectDir = TEMP_DIR.newFolder("sample_proj");

    // Put depA in the correct location
    File depAFile = new File(projectDir, depA.getDestination() + File.separator + depA.getFileName());
    FileUtils.writeStringToFile(depAFile, ThinArchiveTestUtils.getDepAContent());

    // Write the startup-dependencies.json file
    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(projectDir);
    FileUtils.writeStringToFile(startupDependenciesFile, ThinArchiveTestUtils.getRawJSONDepA());

    List<String> jarPaths = new ArrayList<>();
    jarPaths.add(depAFile.getCanonicalPath());
    String jarPathsStr = String.join(",", jarPaths);

    List<String> expectedJarPaths = new ArrayList<>();
    expectedJarPaths.add(jarPaths.get(0));
    String expectedJarPathsStr = String.join(",", expectedJarPaths);

    String resultingJarPathsStr =
        ThinArchiveUtils.replaceLocalPathsWithStoragePaths(projectDir, jarPathsStr, new Props(), log);

    assertEquals(expectedJarPathsStr, resultingJarPathsStr);
  }

  @Test
  public void testReplaceLocalPathsWithStoragePathsMalformedTHIN() throws Exception {
    // This is a test on a project from a THIN archive but with a malformed startup-dependencies.json file
    // we expect the original local file paths to be returned, with no modifications and no exceptions thrown.

    Dependency depA = ThinArchiveTestUtils.getDepA();
    File projectDir = TEMP_DIR.newFolder("sample_proj");

    Props props = new Props();
    props.put(DEPENDENCY_STORAGE_ROOT_PATH_PROP, HDFS_DEP_ROOT_PATH);

    // Put depA in the correct location
    File depAFile = new File(projectDir, depA.getDestination() + File.separator + depA.getFileName());
    FileUtils.writeStringToFile(depAFile, ThinArchiveTestUtils.getDepAContent());

    // Write the startup-dependencies.json file
    File startupDependenciesFile = ThinArchiveUtils.getStartupDependenciesFile(projectDir);
    FileUtils.writeStringToFile(startupDependenciesFile, "MALFORMED JSON BLAHBLAH");

    List<String> jarPaths = new ArrayList<>();
    jarPaths.add(depAFile.getCanonicalPath());
    String jarPathsStr = String.join(",", jarPaths);

    List<String> expectedJarPaths = new ArrayList<>();
    expectedJarPaths.add(jarPaths.get(0));
    String expectedJarPathsStr = String.join(",", expectedJarPaths);

    Logger log = mock(Logger.class);
    String resultingJarPathsStr =
        ThinArchiveUtils.replaceLocalPathsWithStoragePaths(projectDir, jarPathsStr, props, log);

    assertEquals(expectedJarPathsStr, resultingJarPathsStr);
  }

  @Test
  public void testValidateDependencyHashValid() throws Exception {
    File depFile = TEMP_DIR.newFile(ThinArchiveTestUtils.getDepA().getFileName());
    FileUtils.writeStringToFile(depFile, ThinArchiveTestUtils.getDepAContent());
    DependencyFile f = ThinArchiveTestUtils.getDepA().makeDependencyFile(depFile);

    // This should complete without an exception
    ThinArchiveUtils.validateDependencyHash(f);
  }

  @Test(expected = HashNotMatchException.class)
  public void testValidateDependencyHashInvalid() throws Exception {
    File depFile = TEMP_DIR.newFile("dep.jar");

    Dependency details = new Dependency(
        "dep.jar",
        "lib",
        "jar",
        "com.linkedin.test:blahblah:1.0.1",
        "73f018101ec807672cd3b06d5d7a0fc48f54428f"); // This is not the hash of depFile
    DependencyFile f = details.makeDependencyFile(depFile);

    // This should throw an exception because the hashes don't match
    ThinArchiveUtils.validateDependencyHash(f);
  }
}
