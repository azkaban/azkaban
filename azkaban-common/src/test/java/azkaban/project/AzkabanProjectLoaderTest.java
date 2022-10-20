/*
 * Copyright 2017 LinkedIn Corp.
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
 *
 */

package azkaban.project;

import static azkaban.Constants.ConfigurationKeys.PROJECT_TEMP_DIR;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import azkaban.db.DatabaseOperator;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.flow.FlowResourceRecommendation;
import azkaban.metrics.CommonMetrics;
import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidationStatus;
import azkaban.spi.Storage;
import azkaban.storage.ProjectStorageManager;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.test.executions.ThinArchiveTestUtils;
import azkaban.user.User;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.ThinArchiveUtils;
import azkaban.utils.Utils;
import azkaban.utils.ValidatorUtils;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.stubbing.Answer;


public class AzkabanProjectLoaderTest {

  private static final String DIRECTORY_FLOW_REPORT_KEY = "Directory Flow";
  private static final String BASIC_FLOW_YAML_DIR = "basicflowyamltest";
  private static final String BASIC_FLOW_FILE = "basic_flow.flow";
  private static final String PROJECT_ZIP = "Archive.zip";
  private static final String IPv4 = "111.111.111.111";
  private static final String IPv6 = "2607:f0d0:1002:0051:0000:0000:0000:0004";

  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private final int ID = 107;
  private final int VERSION = 10;
  private final Project project = new Project(this.ID, "project1");

  private AzkabanProjectLoader azkabanProjectLoader;
  private ProjectStorageManager projectStorageManager;
  private ProjectLoader projectLoader;
  private ExecutorLoader executorLoader;
  private DatabaseOperator dbOperator;
  private Storage storage;
  private ArchiveUnthinner archiveUnthinner;
  private ValidatorUtils validatorUtils;
  private CommonMetrics commonMetrics;

  @Before
  public void setUp() throws Exception {
    final Props props = new Props();
    props.put(PROJECT_TEMP_DIR, this.TEMP_DIR.getRoot().getAbsolutePath());

    this.projectStorageManager = mock(ProjectStorageManager.class);
    this.projectLoader = mock(ProjectLoader.class);
    this.executorLoader = mock(ExecutorLoader.class);
    this.dbOperator = mock(DatabaseOperator.class);
    this.archiveUnthinner = mock(ArchiveUnthinner.class);
    this.storage = mock(Storage.class);
    this.validatorUtils = mock(ValidatorUtils.class);
    this.commonMetrics = mock(CommonMetrics.class);

    this.azkabanProjectLoader = new AzkabanProjectLoader(props, this.commonMetrics, this.projectLoader,
        this.projectStorageManager, new FlowLoaderFactory(props), this.executorLoader, this.dbOperator, this.storage,
        this.archiveUnthinner, this.validatorUtils);
  }

  @After
  public void cleanUp() {
    this.project.setFlowResourceRecommendations(new HashMap<>());
  }

  @Test
  public void uploadProjectFAT() throws ExecutorManagerException {
    when(this.projectLoader.getLatestProjectVersion(this.project)).thenReturn(this.VERSION);

    final URL resource = requireNonNull(
        getClass().getClassLoader().getResource("sample_flow_01.zip"));
    final File projectZipFile = new File(resource.getPath());
    final User uploader = new User("test_user");

    // to test excluding running versions in args of cleanOlderProjectVersion
    final ExecutableFlow runningFlow = new ExecutableFlow(this.project, new Flow("x"));
    runningFlow.setVersion(this.VERSION);
    when(this.executorLoader.fetchUnfinishedFlowsMetadata())
        .thenReturn(ImmutableMap.of(-1, new Pair<>(null, runningFlow)));

    this.project.setVersion(this.VERSION);
    checkValidationReport(this.azkabanProjectLoader
        .uploadProject(this.project, projectZipFile, "zip", uploader, null,
            IPv4));

    // Flow resource recommendations are newly created and inserted.
    Assert.assertEquals(this.project.getFlowResourceRecommendationMap().size(), 1);
    // startupDependencies should be null - because it does not exist!
    verify(this.projectStorageManager)
        .uploadProject(this.project, this.VERSION + 1, projectZipFile,
            null, uploader, IPv4);
    verify(this.projectLoader).cleanOlderProjectVersion(this.project.getId(), this.VERSION - 2,
        Arrays.asList(this.VERSION));

    // Verify that the archiveUnthinner was never called
    verify(this.archiveUnthinner, never()).validateThinProject(any(), any(), any(), any());
  }

  @Test
  public void flowResourceRecommendationUnchangedWhenUploadProjectFAT() throws ExecutorManagerException {
    final URL resource = requireNonNull(
        getClass().getClassLoader().getResource("sample_flow_01.zip"));
    final File projectZipFile = new File(resource.getPath());
    final User uploader = new User("test_user");

    // same flow as the flow just uploaded
    this.project.setFlowResourceRecommendations(new HashMap<String, FlowResourceRecommendation>(){{
      put("shell_end", new FlowResourceRecommendation(1, 1, "shell_end"));
    }});

    this.project.setVersion(this.VERSION);
    checkValidationReport(this.azkabanProjectLoader
        .uploadProject(this.project, projectZipFile, "zip", uploader, null,
            IPv4));

    Assert.assertEquals(this.project.getFlowResourceRecommendationMap().size(), 1);
  }

  @Test
  public void flowResourceRecommendationChangedWhenUploadProjectFAT() throws ExecutorManagerException {
    final URL resource = requireNonNull(
        getClass().getClassLoader().getResource("sample_flow_01.zip"));
    final File projectZipFile = new File(resource.getPath());
    final User uploader = new User("test_user");

    // different flow as the flow just uploaded
    this.project.setFlowResourceRecommendations(new HashMap<String, FlowResourceRecommendation>(){{
      put("a", new FlowResourceRecommendation(1, 1, "a"));
    }});

    this.project.setVersion(this.VERSION);
    checkValidationReport(this.azkabanProjectLoader
        .uploadProject(this.project, projectZipFile, "zip", uploader, null,
            IPv4));

    Assert.assertEquals(this.project.getFlowResourceRecommendationMap().size(), 2);
  }

  @Test
  public void uploadProjectValidatorRemovedFileTHIN() throws Exception {
    // NOTE!! This test assumes that the thin archive project folder structure defined in
    // ThinArchiveTestUtils.makeSampleThinProjectDirAB() is not modified. If it is modified,
    // this test will have to be updated.
    final User uploader = new User("test_user");
    final File projectZipFile = setupTestTHIN();

    // When archiveUnthinner is called, remove a file from the folder
    doAnswer((Answer<Map<String, ValidationReport>>) invocation -> {
      File libFolder = new File((File) invocation.getArguments()[1], "lib");
      File randomLib = FileUtils.listFiles(libFolder, null, false).iterator().next();

      randomLib.delete();

      ValidationReport r = new ValidationReport();
      r.addRemovedFiles(new HashSet(Arrays.asList(randomLib)));

      Map<String, ValidationReport> resultingReports = new HashMap();
      resultingReports.put("somevalidator", r);
      return resultingReports;
    }).when(this.archiveUnthinner).validateThinProject(any(), any(), any(), any());

    // When uploadProject is called, make sure it has the correctly modified zip being passed in and that
    // the startup-dependencies.json file being passed still exists (it wasn't cleaned up too early).
    doAnswer(invocation -> {
      File zipFileBeingUploaded = (File) invocation.getArguments()[2];
      File unzippedFinalProjFolder = TEMP_DIR.newFolder("finalproj");
      Utils.unzip(new ZipFile(zipFileBeingUploaded), unzippedFinalProjFolder);

      File libFolder = new File(unzippedFinalProjFolder, "lib");
      // Let's check to make sure that file we removed is not included in the final zip
      // the uploadProject method should have noticed that the report mentioned a file was removed
      // and re-zipped the folder. It's also possible that the zipping process removed the empty
      // lib folder, so as long as it does not exist OR is empty we'll pass this test.
      assertTrue(!libFolder.exists()
          || FileUtils.listFiles(libFolder, null, false).size() == 0);

      // Make sure startup-dependencies.json file still exists
      assertTrue(((File) invocation.getArguments()[3]).exists());
      return null;
    }).when(this.projectStorageManager)
        .uploadProject(any(Project.class), anyInt(), any(File.class), any(File.class),
            any(User.class), anyString());

    this.project.setVersion(this.VERSION);
    this.azkabanProjectLoader
        .uploadProject(this.project, projectZipFile, "zip", uploader, null,
            IPv6);

    // Verify that the archiveUnthinner was called
    verify(this.archiveUnthinner).validateThinProject(any(), any(), any(), any());
  }

  @Test
  public void uploadProjectValidatorModifiedFileTHIN() throws Exception {
    // NOTE!! This test assumes that the thin archive project folder structure defined in
    // ThinArchiveTestUtils.makeSampleThinProjectDirAB() is not modified. If it is modified,
    // this test will have to be updated.

    final String writtenModifiedString = "HELLOEVERYONE";
    final User uploader = new User("test_user");
    final File projectZipFile = setupTestTHIN();

    // When archiveUnthinner is called, modify a file in the folder
    doAnswer((Answer<Map<String, ValidationReport>>) invocation -> {
      File libFolder = new File((File) invocation.getArguments()[1], "lib");
      File randomLib = FileUtils.listFiles(libFolder, null, false).iterator().next();

      FileUtils.writeStringToFile(randomLib, writtenModifiedString);

      ValidationReport r = new ValidationReport();
      r.addModifiedFiles(new HashSet(Arrays.asList(randomLib)));

      Map<String, ValidationReport> resultingReports = new HashMap();
      resultingReports.put("somevalidator", r);
      return resultingReports;
    }).when(this.archiveUnthinner).validateThinProject(any(), any(), any(), any());

    // When uploadProject is called, make sure it has the correctly modified zip being passed in and that
    // the startup-dependencies.json file being passed still exists (it wasn't cleaned up too early).
    doAnswer(invocation -> {
      File zipFileBeingUploaded = (File) invocation.getArguments()[2];
      File unzippedFinalProjFolder = TEMP_DIR.newFolder("finalproj");
      Utils.unzip(new ZipFile(zipFileBeingUploaded), unzippedFinalProjFolder);

      File libFolder = new File(unzippedFinalProjFolder, "lib");
      File randomLib = FileUtils.listFiles(libFolder, null, false).iterator().next();
      // Let's check to make sure that file we modified is modified in the final zip
      assertEquals(writtenModifiedString, FileUtils.readFileToString(randomLib));

      // Make sure startup-dependencies.json file still exists
      assertTrue(((File) invocation.getArguments()[3]).exists());
      return null;
    }).when(this.projectStorageManager)
        .uploadProject(any(Project.class), anyInt(), any(File.class), any(File.class),
            any(User.class), anyString());

    this.project.setVersion(this.VERSION);
    this.azkabanProjectLoader
        .uploadProject(this.project, projectZipFile, "zip", uploader, null,
            IPv6);

    // Verify that the archiveUnthinner was called
    verify(this.archiveUnthinner).validateThinProject(any(), any(), any(), any());
  }

  @Test
  public void uploadProjectTHIN() throws Exception {
    final User uploader = new User("test_user");
    final File projectZipFile = setupTestTHIN();

    // When archiveUnthinner is called, ensure it is called with the correct params
    doAnswer((Answer<Map<String, ValidationReport>>) invocation -> {
      File projectFolder = (File) invocation.getArguments()[1];
      File startupDependenciesFile = (File) invocation.getArguments()[2];
      assertEquals(ThinArchiveUtils.getStartupDependenciesFile(projectFolder), startupDependenciesFile);
      return new HashMap();
    }).when(this.archiveUnthinner).validateThinProject(any(), any(), any(), any());

    this.project.setVersion(this.VERSION);
    checkValidationReport(this.azkabanProjectLoader
        .uploadProject(this.project, projectZipFile, "zip", uploader, null,
            IPv6));

    verify(this.projectStorageManager)
        .uploadProject(eq(this.project), eq(this.VERSION + 1), eq(projectZipFile),
            any(File.class), eq(uploader), anyString());
    verify(this.projectLoader).cleanOlderProjectVersion(this.project.getId(), this.VERSION - 2,
        Arrays.asList(this.VERSION));

    // Verify that the archiveUnthinner was called
    verify(this.archiveUnthinner).validateThinProject(any(), any(), any(), any());
  }

  @Test
  public void getProjectFile() throws Exception {
    when(this.projectLoader.getLatestProjectVersion(this.project)).thenReturn(this.VERSION);

    // Run the code
    this.azkabanProjectLoader.getProjectFile(this.project, -1);

    verify(this.projectLoader).getLatestProjectVersion(this.project);
    verify(this.projectStorageManager).getProjectFile(this.ID, this.VERSION);
  }

  @Test
  public void uploadProjectWithYamlFilesFAT() throws Exception {
    final File projectZipFile = ExecutionsTestUtil.getFlowFile(BASIC_FLOW_YAML_DIR, PROJECT_ZIP);
    final int flowVersion = 0;
    final User uploader = new User("test_user");

    when(this.projectLoader.getLatestProjectVersion(this.project)).thenReturn(this.VERSION);
    when(this.projectLoader.getLatestFlowVersion(this.ID, this.VERSION, BASIC_FLOW_FILE))
        .thenReturn(flowVersion);

    checkValidationReport(this.azkabanProjectLoader
        .uploadProject(this.project, projectZipFile, "zip", uploader, null,
            IPv6));

    verify(this.projectStorageManager)
        .uploadProject(this.project, this.VERSION + 1, projectZipFile,
            null, uploader, IPv6);
    verify(this.projectLoader)
        .uploadFlowFile(eq(this.ID), eq(this.VERSION + 1), any(File.class), eq(flowVersion + 1));

  }

  private void checkValidationReport(final Map<String, ValidationReport> validationReportMap) {
    assertThat(validationReportMap.size()).isEqualTo(1);
    assertThat(validationReportMap.containsKey(DIRECTORY_FLOW_REPORT_KEY)).isTrue();
    assertThat(validationReportMap.get(DIRECTORY_FLOW_REPORT_KEY).getStatus()).isEqualTo
        (ValidationStatus.PASS);
  }

  private File setupTestTHIN() throws Exception {
    when(this.projectLoader.getLatestProjectVersion(this.project)).thenReturn(this.VERSION);

    final File thinZipFolder = TEMP_DIR.newFolder("thinproj");
    ThinArchiveTestUtils.makeSampleThinProjectDirAB(thinZipFolder);
    File projectZipFile = TEMP_DIR.newFile("thinzipproj.zip");
    Utils.zipFolderContent(thinZipFolder, projectZipFile);

    // to test excluding running versions in args of cleanOlderProjectVersion
    final ExecutableFlow runningFlow = new ExecutableFlow(this.project, new Flow("x"));
    runningFlow.setVersion(this.VERSION);
    when(this.executorLoader.fetchUnfinishedFlowsMetadata())
        .thenReturn(ImmutableMap.of(-1, new Pair<>(null, runningFlow)));

    return projectZipFile;
  }
}
