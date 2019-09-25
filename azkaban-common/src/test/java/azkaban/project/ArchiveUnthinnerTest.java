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

package azkaban.project;

import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidationStatus;
import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import azkaban.spi.FileOrigin;
import azkaban.spi.FileValidationStatus;
import azkaban.test.executions.ThinArchiveTestUtils;
import azkaban.utils.DependencyTransferException;
import azkaban.utils.DependencyTransferManager;
import azkaban.utils.ValidatorUtils;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.stubbing.Answer;
import org.skyscreamer.jsonassert.JSONAssert;

import static azkaban.test.executions.ThinArchiveTestUtils.*;
import static azkaban.utils.ThinArchiveUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ArchiveUnthinnerTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  public static final String VALIDATION_KEY = "somerandomhash";
  public static final String DEPENDENCY_DOWNLOAD_BASE_URI = "https://example.com/deps/";

  private final Project project = new Project(107, "Test_Project");
  private File projectFolder;
  private File startupDependenciesFile;

  private Dependency depA;
  private Dependency depB;
  private File depAInArtifactory;
  private File depBInArtifactory;
  private File startupDepsInProject;
  private File depAInProject;
  private File depBInProject;

  private Set<Dependency> depSetAB;
  private Set<Dependency> depSetA;
  private Set<Dependency> depSetB;

  private ArchiveUnthinner archiveUnthinner;
  private ValidatorUtils validatorUtils;
  private JdbcDependencyManager jdbcDependencyManager;
  private DependencyTransferManager dependencyTransferManager;

  private Map<String, ValidationReport> defaultBaseReport;

  @Before
  public void setUp() throws Exception {
    this.validatorUtils = mock(ValidatorUtils.class);
    this.jdbcDependencyManager = mock(JdbcDependencyManager.class);
    this.dependencyTransferManager = mock(DependencyTransferManager.class);
    doReturn(true).when(this.dependencyTransferManager).storageOriginEnabled();
    doReturn(true).when(this.dependencyTransferManager).remoteOriginEnabled();

    this.archiveUnthinner = new ArchiveUnthinner(this.validatorUtils,
        this.jdbcDependencyManager, this.dependencyTransferManager);

    // Create test project directory
    // ../
    // ../lib/some-snapshot.jar
    // ../app-meta/startup-dependencies.json
    projectFolder = TEMP_DIR.newFolder("testproj");
    startupDependenciesFile = getStartupDependenciesFile(this.projectFolder);
    ThinArchiveTestUtils.makeSampleThinProjectDirAB(projectFolder);

    // Setup sample dependencies
    depA = ThinArchiveTestUtils.getDepA();
    depB = ThinArchiveTestUtils.getDepB();
    depAInArtifactory = TEMP_DIR.newFile(depA.getFileName());
    depBInArtifactory = TEMP_DIR.newFile(depB.getFileName());
    FileUtils.writeStringToFile(depAInArtifactory, ThinArchiveTestUtils.getDepAContent());
    FileUtils.writeStringToFile(depBInArtifactory, ThinArchiveTestUtils.getDepBContent());

    // Get objects for commonly referenced files in tests
    startupDepsInProject = getStartupDependenciesFile(projectFolder);
    depAInProject = new File(projectFolder, depA.getDestination() + File.separator + depA.getFileName());
    depBInProject = new File(projectFolder, depB.getDestination() + File.separator + depB.getFileName());

    // Get commonly used sets
    depSetA = ThinArchiveTestUtils.getDepSetA();
    depSetB = ThinArchiveTestUtils.getDepSetB();
    depSetAB = ThinArchiveTestUtils.getDepSetAB();

    // When downloadDependency() is called, write the content to the file as if it was downloaded
    // NOTE: This is only set up to work for depA and depB right now!!!
    doAnswer(invocation -> {
      ((Set<DependencyFile>) invocation.getArguments()[0]).forEach(destDep -> {
        String contentToWrite = destDep.equals(depA) ?
            ThinArchiveTestUtils.getDepAContent() :
            ThinArchiveTestUtils.getDepBContent();

        try {
          FileUtils.writeStringToFile(destDep.getFile(), contentToWrite);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      return null;
    }).when(this.dependencyTransferManager).downloadAllDependencies(any(Set.class), eq(FileOrigin.REMOTE));

    // When the unthinner attempts to get a validationKey for the project, return our sample one.
    when(this.validatorUtils.getCacheKey(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(VALIDATION_KEY);

    // Set up a default report map with a single empty report (the cached report)
    defaultBaseReport = new HashMap<>();
    defaultBaseReport.put(ArchiveUnthinner.UNTHINNING_CACHED_VALIDATOR_REPORT_NAME, new ValidationReport());
  }

  private Map<String, ValidationReport> runUnthinner() {
    return this.archiveUnthinner.validateProjectAndPersistDependencies(this.project, this.projectFolder,
        startupDependenciesFile, null);
  }

  @Test(expected = ProjectManagerException.class)
  public void testDisabled() {
    // Ensure that if the
    DependencyTransferManager disabledTransferManager = mock(DependencyTransferManager.class);
    doReturn(false).when(disabledTransferManager).remoteOriginEnabled();
    doReturn(false).when(disabledTransferManager).storageOriginEnabled();

    ArchiveUnthinner disabledUnthinner = new ArchiveUnthinner(this.validatorUtils,
        this.jdbcDependencyManager, disabledTransferManager);

    // Try to validate a project (this should fail!)
    disabledUnthinner.validateProjectAndPersistDependencies(this.project, this.projectFolder,
        startupDependenciesFile, null);
  }

  @Test
  public void testAllNew() throws Exception {
    // Indicate that the both dependencies are NEW, forcing them to be downloaded
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.NEW);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return an empty map (indicating that
    // no validators are loaded - and no validation modifications have been made)
    when(this.validatorUtils.validateProject(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(new HashMap<>());

    // Indicate both deps persisted successfully (we return the exact same set of dependencies that was passed in)
    doAnswer(invocation -> invocation.getArguments()[0])
        .when(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetAB), eq(FileOrigin.STORAGE));

    // Run the ArchiveUnthinner!
    Map<String, ValidationReport> result = runUnthinner();

    // Verify that ValidationReport is as expected (empty)
    assertEquals(result, defaultBaseReport);

    // Verify that both dependencies were persisted to storage
    verify(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetAB), eq(FileOrigin.STORAGE));

    // Verify that both dependencies were added to DB as VALID
    Map<Dependency, FileValidationStatus> expectedStatuses = new HashMap();
    expectedStatuses.put(depA, FileValidationStatus.VALID);
    expectedStatuses.put(depB, FileValidationStatus.VALID);
    verify(this.jdbcDependencyManager).updateValidationStatuses(expectedStatuses, VALIDATION_KEY);

    // Verify that dependencies were removed from project /lib folder and only original snapshot jar remains
    assertEquals(1, new File(projectFolder, depA.getDestination()).listFiles().length);

    // Verify that the startup-dependencies.json file is NOT modified
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestUtils.getRawJSONDepsAB(), finalJSON, false);
  }

  @Test
  public void testAllNewUnrelatedModifyRemove() throws Exception {
    // Indicate that the both dependencies are NEW, forcing them to be downloaded
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.NEW);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return a report that indicates
    // the validator modified a non-dependency file and removed a non-dependency file
    File randomFileToRemove = new File(this.projectFolder, "imnotreal.txt");
    File randomFileToModify = new File(this.projectFolder, "lib/imnotreal.jar");
    Set<File> removedFiles = new HashSet();
    removedFiles.add(randomFileToRemove);
    Set<File> modifiedFiles = new HashSet();
    modifiedFiles.add(randomFileToModify);
    ValidationReport sampleReport = new ValidationReport();
    sampleReport.addRemovedFiles(removedFiles);
    sampleReport.addModifiedFiles(modifiedFiles);

    Map<String, ValidationReport> allReports = new HashMap<>();
    allReports.put("sample", sampleReport);
    when(this.validatorUtils.validateProject(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(allReports);

    // Indicate both deps persisted successfully (we return the exact same set of dependencies that was passed in)
    doAnswer(invocation -> invocation.getArguments()[0])
        .when(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetAB), eq(FileOrigin.STORAGE));

    // Run the ArchiveUnthinner!
    Map<String, ValidationReport> result = runUnthinner();

    // Verify that ValidationReport is as expected (has one empty cached report)
    defaultBaseReport.put("sample", sampleReport);
    assertEquals(result, defaultBaseReport);

    // Verify that both dependencies were persisted to storage
    verify(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetAB), eq(FileOrigin.STORAGE));

    // Verify that both dependencies were added to DB as VALID
    Map<Dependency, FileValidationStatus> expectedStatuses = new HashMap();
    expectedStatuses.put(depA, FileValidationStatus.VALID);
    expectedStatuses.put(depB, FileValidationStatus.VALID);
    verify(this.jdbcDependencyManager).updateValidationStatuses(expectedStatuses, VALIDATION_KEY);

    // Verify that dependencies were removed from project /lib folder and only original snapshot jar remains
    assertEquals(1, new File(projectFolder, depA.getDestination()).listFiles().length);

    // Verify that the startup-dependencies.json file is NOT modified
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestUtils.getRawJSONDepsAB(), finalJSON, false);
  }

  @Test
  public void testCachedValid() throws Exception {
    // Indicate that the depA is cached VALID, but depB is NEW (forcing depB to be downloaded)
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.VALID);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return an empty map (indicating that
    // no validators are loaded - and no validation modifications have been made)
    when(this.validatorUtils.validateProject(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(new HashMap<>());

    // Indicate ONLY depB persisted successfully (we return the exact same set of dependencies that was passed in)
    doAnswer(invocation -> invocation.getArguments()[0])
        .when(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetB), eq(FileOrigin.STORAGE));

    // Run the ArchiveUnthinner!
    Map<String, ValidationReport> result = runUnthinner();

    // Verify that ValidationReport is as expected (has one empty cached report)
    assertEquals(result, defaultBaseReport);

    // Verify that ONLY depB was persisted to storage
    verify(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetB), eq(FileOrigin.STORAGE));

    // Verify that ONLY depB was added to DB as VALID
    Map<Dependency, FileValidationStatus> expectedStatuses = new HashMap();
    expectedStatuses.put(depB, FileValidationStatus.VALID);
    verify(this.jdbcDependencyManager).updateValidationStatuses(expectedStatuses, VALIDATION_KEY);

    // Verify that no dependencies were added to project /lib folder and only original snapshot jar remains
    assertEquals(1, new File(projectFolder, depA.getDestination()).listFiles().length);

    // Verify that the startup-dependencies.json file is NOT modified
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestUtils.getRawJSONDepsAB(), finalJSON, false);
  }

  @Test
  public void testCachedRemoved() throws Exception {
    // Indicate that depA is cached REMOVED, but depB is NEW (forcing depB to be downloaded)
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.REMOVED);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return an empty map (indicating that
    // no validators are loaded - and no validation modifications have been made)
    when(this.validatorUtils.validateProject(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(new HashMap<>());

    // Indicate ONLY depB persisted successfully (we return the exact same set of dependencies that was passed in)
    doAnswer(invocation -> invocation.getArguments()[0])
        .when(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetB), eq(FileOrigin.STORAGE));

    // Run the ArchiveUnthinner!
    Map<String, ValidationReport> result = runUnthinner();

    // Verify that ValidationReport indicates the correct modified file (startup dependencies json)
    // and one warning for the cached removed file
    Set<File> modifiedFiles = new HashSet();
    modifiedFiles.add(startupDepsInProject);
    assertEquals(1, result.get(ArchiveUnthinner.UNTHINNING_CACHED_VALIDATOR_REPORT_NAME).getWarningMsgs().size());
    assertEquals(modifiedFiles, result.get(ArchiveUnthinner.UNTHINNING_CACHED_VALIDATOR_REPORT_NAME).getModifiedFiles());

    // Verify that ONLY depB was persisted to storage
    verify(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetB), eq(FileOrigin.STORAGE));

    // Verify that ONLY depB was added to DB as VALID
    Map<Dependency, FileValidationStatus> expectedStatuses = new HashMap();
    expectedStatuses.put(depB, FileValidationStatus.VALID);
    verify(this.jdbcDependencyManager).updateValidationStatuses(expectedStatuses, VALIDATION_KEY);

    // Verify that no dependencies were added to project /lib folder and only original snapshot jar remains
    assertEquals(1, new File(projectFolder, depA.getDestination()).listFiles().length);

    // Verify that the startup-dependencies.json file now contains ONLY depB
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestUtils.getRawJSONDepB(), finalJSON, false);
  }

  @Test
  public void testValidatorDeleteFile() throws Exception {
    // Indicate that the both dependencies are NEW, forcing them to be downloaded
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.NEW);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return a report indicating that the depA jar
    // was removed.
    Set<File> removedFiles = new HashSet();
    removedFiles.add(depAInProject);

    doAnswer((Answer<Map>) invocation -> {
      depAInProject.delete();

      ValidationReport sampleReport = new ValidationReport();
      sampleReport.addRemovedFiles(removedFiles);

      Map<String, ValidationReport> allReports = new HashMap<>();
      allReports.put("sample", sampleReport);

      return allReports;
    }).when(this.validatorUtils).validateProject(eq(this.project), eq(this.projectFolder), any());

    // Indicate ONLY depB persisted successfully (we return the exact same set of dependencies that was passed in)
    doAnswer(invocation -> invocation.getArguments()[0])
        .when(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetB), eq(FileOrigin.STORAGE));

    // Run the ArchiveUnthinner!
    Map<String, ValidationReport> result = runUnthinner();

    // Verify that ValidationReport indicates the correct removed file (an no modified files).
    assertEquals(removedFiles, result.get("sample").getRemovedFiles());
    assertEquals(0, result.get("sample").getModifiedFiles().size());

    // Verify that ONLY depB was persisted to storage
    verify(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetB), eq(FileOrigin.STORAGE));

    // Verify that depA was added to DB as REMOVED and depB was added as VALID
    Map<Dependency, FileValidationStatus> expectedStatuses = new HashMap();
    expectedStatuses.put(depA, FileValidationStatus.REMOVED);
    expectedStatuses.put(depB, FileValidationStatus.VALID);
    verify(this.jdbcDependencyManager).updateValidationStatuses(expectedStatuses, VALIDATION_KEY);

    // Verify that no dependencies were added to project /lib folder and only original snapshot jar remains
    assertEquals(1, new File(projectFolder, depA.getDestination()).listFiles().length);

    // Verify that the startup-dependencies.json file now contains ONLY depB
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestUtils.getRawJSONDepB(), finalJSON, false);
  }

  @Test
  public void testValidatorModifyFile() throws Exception {
    // Indicate that the both dependencies are NEW, forcing them to be downloaded
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.NEW);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return a report indicating that the depA jar
    // was modified.
    Set<File> modifiedFiles = new HashSet();
    modifiedFiles.add(depAInProject);

    doAnswer((Answer<Map>) invocation -> {
      ValidationReport sampleReport = new ValidationReport();
      sampleReport.addModifiedFiles(modifiedFiles);

      Map<String, ValidationReport> allReports = new HashMap<>();
      allReports.put("sample", sampleReport);

      return allReports;
    }).when(this.validatorUtils).validateProject(eq(this.project), eq(this.projectFolder), any());

    // Indicate ONLY depB persisted successfully (we return the exact same set of dependencies that was passed in)
    doAnswer(invocation -> invocation.getArguments()[0])
        .when(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetB), eq(FileOrigin.STORAGE));

    // Run the ArchiveUnthinner!
    Map<String, ValidationReport> result = runUnthinner();

    // Verify that ValidationReport indicates the correct modified file (and no removed files).
    assertEquals(modifiedFiles, result.get("sample").getModifiedFiles());
    assertEquals(0, result.get("sample").getRemovedFiles().size());

    // Verify that ONLY depB was persisted to storage
    verify(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetB), eq(FileOrigin.STORAGE));

    // Verify that ONLY depB was added to DB as VALID
    Map<Dependency, FileValidationStatus> expectedStatuses = new HashMap();
    expectedStatuses.put(depB, FileValidationStatus.VALID);
    verify(this.jdbcDependencyManager).updateValidationStatuses(expectedStatuses, VALIDATION_KEY);

    // Verify that depA remains in the projectFolder (total of two jars)
    assertEquals(2, new File(projectFolder, depA.getDestination()).listFiles().length);
    assertTrue(depAInProject.exists());

    // Verify that the startup-dependencies.json file now contains ONLY depB
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestUtils.getRawJSONDepB(), finalJSON, false);
  }

  @Test
  public void testUnsuccessfulPersist() throws Exception {
    // Indicate that the both dependencies are NEW, forcing them to be downloaded
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.NEW);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return an empty map (indicating that the
    // validator found no errors and made no changes to the project)
    when(this.validatorUtils.validateProject(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(new HashMap<>());

    // Indicate ONLY depA persisted successfully, depB may still be OPEN
    doAnswer(invocation -> {
      return ((Set<DependencyFile>) invocation.getArguments()[0])
          .stream()
          .filter(f -> f.getFileName().equals(depA.getFileName()))
          .collect(Collectors.toSet());
    }).when(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetAB), eq(FileOrigin.STORAGE));

    // Run the ArchiveUnthinner!
    Map<String, ValidationReport> result = runUnthinner();

    // Verify that ValidationReport contains modified startup dependencies file and depB
    Set<File> modifiedFiles = new HashSet();
    modifiedFiles.add(startupDepsInProject);
    modifiedFiles.add(depBInProject);
    assertEquals(modifiedFiles, result.get(ArchiveUnthinner.UNTHINNING_CACHED_VALIDATOR_REPORT_NAME).getModifiedFiles());

    // Verify that both dependencies were ATTEMPTED to be persisted to storage
    verify(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetAB), eq(FileOrigin.STORAGE));

    // Verify that ONLY depA was added to the DB as VALID
    Map<Dependency, FileValidationStatus> expectedStatuses = new HashMap();
    expectedStatuses.put(depA, FileValidationStatus.VALID);
    verify(this.jdbcDependencyManager).updateValidationStatuses(expectedStatuses, VALIDATION_KEY);

    // Verify that depB remains in the projectFolder (total of two jars)
    // depB was not guaranteed to be persisted to storage, thus for safety we keep it in the zip in-case
    // the process that is uploading it fails to successfully complete the upload and the dependency is
    // not available from storage at runtime.
    File depBInProject = new File(projectFolder, depB.getDestination() + File.separator + depB.getFileName());
    assertEquals(1, new File(projectFolder, "lib").listFiles().length);
    assertTrue(depBInProject.exists());

    // Verify that the startup-dependencies.json file now contains ONLY depA
    String finalJSON = FileUtils.readFileToString(startupDependenciesFile);
    JSONAssert.assertEquals(ThinArchiveTestUtils.getRawJSONDepA(), finalJSON, false);
  }

  @Test(expected = ProjectManagerException.class)
  public void testErrorPersist() throws Exception {
    // Indicate that the both dependencies are NEW, forcing them to be downloaded
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.NEW);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return an empty map (indicating that the
    // validator found no errors and made no changes to the project)
    when(this.validatorUtils.validateProject(eq(this.project), eq(this.projectFolder), any()))
        .thenReturn(new HashMap<>());

    // Throw an exception to indicate something went wrong during one of the uploads.
    doThrow(new DependencyTransferException())
        .when(this.dependencyTransferManager).uploadAllDependencies(depSetEq(depSetAB), eq(FileOrigin.STORAGE));

    // Run the ArchiveUnthinner!
    runUnthinner();
  }

  @Test(expected = ProjectManagerException.class)
  public void testErrorDownload() throws Exception {
    // Indicate that the both dependencies are NEW, forcing them to be downloaded
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.NEW);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // Remove the default mock for download handling
    reset(this.dependencyTransferManager);

    // When we attempt to download the dependencies, throw an error
    doThrow(new DependencyTransferException())
        .when(this.dependencyTransferManager).downloadAllDependencies(depSetEq(depSetAB), eq(FileOrigin.REMOTE));

    // Run the ArchiveUnthinner!
    runUnthinner();
  }

  @Test
  public void testReportWithError() throws Exception {
    // Indicate that the both dependencies are NEW, forcing them to be downloaded
    Map<Dependency, FileValidationStatus> sampleValidationStatuses = new HashMap();
    sampleValidationStatuses.put(depA, FileValidationStatus.NEW);
    sampleValidationStatuses.put(depB, FileValidationStatus.NEW);
    when(this.jdbcDependencyManager.getValidationStatuses(any(), eq(VALIDATION_KEY)))
        .thenReturn(sampleValidationStatuses);

    // When the unthinner attempts to validate the project, return a report with ValidationStatus.ERROR
    doAnswer((Answer<Map>) invocation -> {
      String someErrorMsg = "Big problem!";
      Set<String> errMsgs = new HashSet();
      errMsgs.add(someErrorMsg);

      ValidationReport sampleReport = new ValidationReport();
      sampleReport.addErrorMsgs(errMsgs);

      Map<String, ValidationReport> allReports = new HashMap<>();
      allReports.put("sample", sampleReport);

      return allReports;
    }).when(this.validatorUtils).validateProject(eq(this.project), eq(this.projectFolder), any());

    // Run the ArchiveUnthinner!
    Map<String, ValidationReport> result = runUnthinner();

    // Verify that ValidationReport has an ERROR status.
    assertEquals(ValidationStatus.ERROR, result.get("sample").getStatus());

    // Verify that no dependencies were persisted
    verify(this.dependencyTransferManager, never()).uploadAllDependencies(any(), any());

    // Verify that nothing was updated in DB
    verify(this.jdbcDependencyManager, never()).updateValidationStatuses(any(), any());
  }
}
