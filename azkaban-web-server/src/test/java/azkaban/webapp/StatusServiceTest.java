/*
 * Copyright 2021 LinkedIn Corp.
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
package azkaban.webapp;

import azkaban.Constants;
import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.DispatchMethod;
import azkaban.ServiceProvider;
import azkaban.db.DatabaseOperator;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.container.ContainerJobTypeCriteria;
import azkaban.executor.container.ContainerRampUpCriteria;
import azkaban.executor.container.ContainerizedDispatchManager;
import azkaban.imagemgmt.dto.ImageVersionMetadataResponseDTO;
import azkaban.imagemgmt.dto.ImageVersionMetadataResponseDTO.RampupMetadata;
import azkaban.imagemgmt.models.ImageRampup.StabilityTag;
import azkaban.imagemgmt.models.ImageVersion.State;
import azkaban.imagemgmt.services.ImageVersionMetadataService;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import azkaban.webapp.servlet.StatusServlet;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import java.io.IOException;
import java.util.Collections;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class StatusServiceTest {

  @Mock
  private ContainerizedDispatchManager executorManager;
  @Mock
  private Injector injector;
  @Mock
  private ExecutorLoader executorLoader;
  @Mock
  private DatabaseOperator dbOperator;
  @Mock
  private ImageVersionMetadataService imageVersionMetadataService;
  private final Props props = new Props();
  private StatusService statusService;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    ServiceProvider.SERVICE_PROVIDER.unsetInjector();
    ServiceProvider.SERVICE_PROVIDER.setInjector(this.injector);
    Mockito.when(this.injector.getInstance(ImageVersionMetadataService.class))
        .thenReturn(this.imageVersionMetadataService);
    Mockito.when(this.injector.getInstance(ExecutorManagerAdapter.class))
        .thenReturn(this.executorManager);

    this.props.put(ContainerizedDispatchManagerProperties.CONTAINERIZED_JOBTYPE_ALLOWLIST,
        "jobType1, jobType2");
    this.props.put(ContainerizedDispatchManagerProperties.CONTAINERIZED_RAMPUP,
        "50");
    ImageVersionMetadataResponseDTO imageVersionMetadataResponseDTO = new ImageVersionMetadataResponseDTO(
        "0.0.7", State.ACTIVE, "registry/myPath",
        Collections.singletonList(new RampupMetadata("0.0.7", 100, StabilityTag.STABLE)), "");

    Mockito.when(this.dbOperator.query(Mockito.any(), Mockito.any())).thenReturn(true);
    Mockito.when(this.imageVersionMetadataService.getVersionMetadataForAllImageTypes())
        .thenReturn(ImmutableMap.of("myJobType", imageVersionMetadataResponseDTO));
    Mockito.when(this.executorManager.getContainerRampUpCriteria())
        .thenReturn(new ContainerRampUpCriteria(this.props));
    Mockito.when(this.executorManager.getContainerJobTypeCriteria())
        .thenReturn(new ContainerJobTypeCriteria(this.props));
    Mockito.when(this.executorLoader.fetchActiveExecutors()).thenReturn(
        Collections.singletonList(new Executor(7, "0.0.0.0", 12345, true)));
    this.statusService = Mockito.spy(new StatusService(this.props, this.executorLoader,
        this.dbOperator));
    Mockito.doReturn("0.0.7").when(this.statusService).getVersion();
    Mockito.doReturn("123").when(this.statusService).getPid();
    Mockito.doReturn("/myDir").when(this.statusService).getInstallationPath();
    Mockito.doReturn(12345L).when(this.statusService).getUsedMemory();
    Mockito.doReturn(12345L).when(this.statusService).getMaxMemory();
  }

  @Test
  public void testClusterStatus() throws IOException {
    ObjectMapper objectMapper = StatusServlet.getObjectMapper();
    String clusterStatusString = objectMapper.writerWithDefaultPrettyPrinter()
        .writeValueAsString(this.statusService.getStatus());
    String expectedClusterStatusString = TestUtils
        .readResource("cluster_status", this).trim();
    Assert.assertEquals(expectedClusterStatusString, clusterStatusString);

    this.props.put(Constants.ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD,
        DispatchMethod.CONTAINERIZED.name());
    String containerizedClusterStatusString = objectMapper.writerWithDefaultPrettyPrinter()
        .writeValueAsString(this.statusService.getStatus());
    String expectedContainerizedClusterStatusString = TestUtils
        .readResource("containerized_cluster_status", this).trim();
    Assert.assertEquals(expectedContainerizedClusterStatusString, containerizedClusterStatusString);
  }
}
