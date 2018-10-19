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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.RunningExecutions;
import azkaban.flow.Flow;
import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidationStatus;
import azkaban.storage.StorageManager;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.user.User;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AzkabanProjectLoaderTest {

  private static final String DIRECTORY_FLOW_REPORT_KEY = "Directory Flow";
  private static final String BASIC_FLOW_YAML_DIR = "basicflowyamltest";
  private static final String BASIC_FLOW_FILE = "basic_flow.flow";
  private static final String PROJECT_ZIP = "Archive.zip";

  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private final int ID = 107;
  private final int VERSION = 10;
  private final Project project = new Project(this.ID, "project1");

  private AzkabanProjectLoader azkabanProjectLoader;
  private StorageManager storageManager;
  private ProjectLoader projectLoader;
  private RunningExecutions runningExecutions;

  @Before
  public void setUp() throws Exception {
    final Props props = new Props();
    props.put(PROJECT_TEMP_DIR, this.TEMP_DIR.getRoot().getAbsolutePath());

    this.storageManager = mock(StorageManager.class);
    this.projectLoader = mock(ProjectLoader.class);
    this.runningExecutions = new RunningExecutions();

    this.azkabanProjectLoader = new AzkabanProjectLoader(props, this.projectLoader,
        this.storageManager, new FlowLoaderFactory(props), this.runningExecutions);
  }

  @Test
  public void uploadProject() {
    when(this.projectLoader.getLatestProjectVersion(this.project)).thenReturn(this.VERSION);

    final URL resource = requireNonNull(
        getClass().getClassLoader().getResource("sample_flow_01.zip"));
    final File projectZipFile = new File(resource.getPath());
    final User uploader = new User("test_user");

    // to test excluding running versions in args of cleanOlderProjectVersion
    final ExecutableFlow runningFlow = new ExecutableFlow(this.project, new Flow("x"));
    runningFlow.setVersion(this.VERSION);
    this.runningExecutions.get().put(-1, new Pair<>(null, runningFlow));

    this.project.setVersion(this.VERSION);
    checkValidationReport(this.azkabanProjectLoader
        .uploadProject(this.project, projectZipFile, "zip", uploader, null));

    verify(this.storageManager)
        .uploadProject(this.project, this.VERSION + 1, projectZipFile, uploader);
    verify(this.projectLoader).cleanOlderProjectVersion(this.project.getId(), this.VERSION - 3,
        Arrays.asList(this.VERSION));
  }

  @Test
  public void getProjectFile() throws Exception {
    when(this.projectLoader.getLatestProjectVersion(this.project)).thenReturn(this.VERSION);

    // Run the code
    this.azkabanProjectLoader.getProjectFile(this.project, -1);

    verify(this.projectLoader).getLatestProjectVersion(this.project);
    verify(this.storageManager).getProjectFile(this.ID, this.VERSION);
  }

  @Test
  public void uploadProjectWithYamlFiles() throws Exception {
    final File projectZipFile = ExecutionsTestUtil.getFlowFile(BASIC_FLOW_YAML_DIR, PROJECT_ZIP);
    final int flowVersion = 0;
    final User uploader = new User("test_user");

    when(this.projectLoader.getLatestProjectVersion(this.project)).thenReturn(this.VERSION);
    when(this.projectLoader.getLatestFlowVersion(this.ID, this.VERSION, BASIC_FLOW_FILE))
        .thenReturn(flowVersion);

    checkValidationReport(this.azkabanProjectLoader
        .uploadProject(this.project, projectZipFile, "zip", uploader, null));

    verify(this.storageManager)
        .uploadProject(this.project, this.VERSION + 1, projectZipFile, uploader);
    verify(this.projectLoader)
        .uploadFlowFile(eq(this.ID), eq(this.VERSION + 1), any(File.class), eq(flowVersion + 1));

  }

  private void checkValidationReport(final Map<String, ValidationReport> validationReportMap) {
    assertThat(validationReportMap.size()).isEqualTo(1);
    assertThat(validationReportMap.containsKey(DIRECTORY_FLOW_REPORT_KEY)).isTrue();
    assertThat(validationReportMap.get(DIRECTORY_FLOW_REPORT_KEY).getStatus()).isEqualTo
        (ValidationStatus.PASS);
  }
}
