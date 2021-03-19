/*
 * Copyright 2020 LinkedIn Corp.
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
package azkaban.executor.container;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_EVENT_REPORTING_CLASS_PARAM;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_EVENT_REPORTING_ENABLED;
import static azkaban.Constants.ImageMgmtConstants.AZKABAN_BASE_IMAGE;
import static azkaban.Constants.ImageMgmtConstants.AZKABAN_CONFIG;
import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.AzkabanCommonModule;
import azkaban.Constants;
import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.Constants.FlowParameters;
import azkaban.DispatchMethod;
import azkaban.db.DatabaseOperator;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.Status;
import azkaban.imagemgmt.converters.Converter;
import azkaban.imagemgmt.converters.ImageRampupPlanConverter;
import azkaban.imagemgmt.converters.ImageTypeConverter;
import azkaban.imagemgmt.converters.ImageVersionConverter;
import azkaban.imagemgmt.daos.ImageRampupDao;
import azkaban.imagemgmt.daos.ImageRampupDaoImpl;
import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.daos.ImageTypeDaoImpl;
import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.daos.ImageVersionDaoImpl;
import azkaban.imagemgmt.dto.ImageRampupPlanRequestDTO;
import azkaban.imagemgmt.dto.ImageRampupPlanResponseDTO;
import azkaban.imagemgmt.dto.ImageTypeDTO;
import azkaban.imagemgmt.dto.ImageVersionDTO;
import azkaban.imagemgmt.models.ImageRampupPlan;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.ImageVersion.State;
import azkaban.imagemgmt.rampup.ImageRampupManager;
import azkaban.imagemgmt.rampup.ImageRampupManagerImpl;
import azkaban.imagemgmt.version.JdbcVersionSetLoader;
import azkaban.imagemgmt.version.VersionInfo;
import azkaban.imagemgmt.version.VersionSet;
import azkaban.imagemgmt.version.VersionSetBuilder;
import azkaban.imagemgmt.version.VersionSetLoader;
import azkaban.test.Utils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.util.Yaml;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class covers unit tests for KubernetesContainerizedImpl class.
 */
public class KubernetesContainerizedImplTest {

  private static final Props props = new Props();
  private KubernetesContainerizedImpl kubernetesContainerizedImpl;
  private ExecutorLoader executorLoader;
  private static DatabaseOperator dbOperator;
  private VersionSetLoader loader;
  private static ImageRampupManager imageRampupManager;
  private static ImageTypeDao imageTypeDao;
  private static ImageVersionDao imageVersionDao;
  private static ImageRampupDao imageRampupDao;
  private static final String TEST_JSON_DIR = "image_management/k8s_dispatch_test";
  private static final String CPU_REQUESTED_IN_PROPS = "2";
  private static final String MEMORY_REQUESTED_IN_PROPS = "4Gi";
  private static Converter<ImageTypeDTO, ImageTypeDTO,
      ImageType> imageTypeConverter;
  private static Converter<ImageVersionDTO, ImageVersionDTO,
      ImageVersion> imageVersionConverter;
  private static Converter<ImageRampupPlanRequestDTO, ImageRampupPlanResponseDTO,
      ImageRampupPlan> imageRampupPlanConverter;

  private static final Logger log = LoggerFactory.getLogger(KubernetesContainerizedImplTest.class);

  @BeforeClass
  public static void setUp() throws Exception {
    dbOperator = Utils.initTestDB();
    setupImageTables();
  }

  @AfterClass
  public static void destroyDB() {
    try {
      dbOperator.update("DROP ALL OBJECTS");
      dbOperator.update("SHUTDOWN");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  @Before
  public void setup() throws Exception {
    this.props.put(ContainerizedDispatchManagerProperties.KUBERNETES_NAMESPACE, "dev-namespace");
    this.props.put(ContainerizedDispatchManagerProperties.KUBERNETES_KUBE_CONFIG_PATH, "src/test"
        + "/resources/container/kubeconfig");
    this.props.put(ContainerizedDispatchManagerProperties.KUBERNETES_SERVICE_REQUIRED, "true");
    this.props.put(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_CPU_REQUEST,
        CPU_REQUESTED_IN_PROPS);
    this.props.put(ContainerizedDispatchManagerProperties.KUBERNETES_FLOW_CONTAINER_MEMORY_REQUEST
        , MEMORY_REQUESTED_IN_PROPS);
    this.executorLoader = mock(ExecutorLoader.class);
    this.loader = new JdbcVersionSetLoader(this.dbOperator);
    this.kubernetesContainerizedImpl = new KubernetesContainerizedImpl(this.props,
        this.executorLoader, this.loader, this.imageRampupManager);
  }

  /**
   * This test is used to verify that if cpu and memory for a flow container is requested from flow
   * parameter then that is given more precedence over system configuration.
   *
   * @throws Exception
   */
  @Test
  public void testCPUAndMemoryRequestedInFlowParam() throws Exception {
    final Map<String, String> flowParam = new HashMap<>();
    final String cpuRequestedInFlowParam = "3";
    final String memoryRequestedInFlowParam = "3Gi";
    flowParam.put(FlowParameters.FLOW_PARAM_FLOW_CONTAINER_CPU_REQUEST, cpuRequestedInFlowParam);
    flowParam
        .put(FlowParameters.FLOW_PARAM_FLOW_CONTAINER_MEMORY_REQUEST, memoryRequestedInFlowParam);
    assert (this.kubernetesContainerizedImpl.getFlowContainerCPURequest(flowParam))
        .equals(cpuRequestedInFlowParam);
    assert (this.kubernetesContainerizedImpl.getFlowContainerMemoryRequest(flowParam))
        .equals(memoryRequestedInFlowParam);
  }

  /**
   * This test is used to verify that if CPU and memory for a flow container is not requested in
   * flow param but defined in system configuration then that is used.
   *
   * @throws Exception
   */
  @Test
  public void testCPUAndMemoryRequestedFromProperties() throws Exception {
    final Map<String, String> flowParam = new HashMap<>();
    assert (this.kubernetesContainerizedImpl.getFlowContainerCPURequest(flowParam))
        .equals(CPU_REQUESTED_IN_PROPS);
    assert (this.kubernetesContainerizedImpl.getFlowContainerMemoryRequest(flowParam))
        .equals(MEMORY_REQUESTED_IN_PROPS);
  }

  @Test
  public void testJobTypesInFlow() throws Exception {
    final ExecutableFlow flow = createTestFlow();
    flow.setSubmitUser("testUser1");
    flow.setStatus(Status.PREPARING);
    flow.setSubmitTime(System.currentTimeMillis());
    flow.setExecutionId(0);
    final TreeSet<String> jobTypes = ContainerImplUtils.getJobTypesForFlow(flow);
    assertThat(jobTypes.size()).isEqualTo(1);
  }

  @Test
  public void testPodConstruction() throws Exception {
    final ExecutableFlow flow = createFlowWithMultipleJobtypes();
    flow.setExecutionId(1);
    when(this.executorLoader.fetchExecutableFlow(flow.getExecutionId())).thenReturn(flow);
    when(imageRampupManager.getVersionByImageTypes(any(), any(Set.class)))
        .thenReturn(getVersionMap());
    final TreeSet<String> jobTypes = ContainerImplUtils.getJobTypesForFlow(flow);
    assert (jobTypes.contains("command"));
    assert (jobTypes.contains("hadoopJava"));
    assert (jobTypes.contains("spark"));
    log.info("Jobtypes for flow {} are: {}", flow.getFlowId(), jobTypes);

    final Map<String, String> flowParam = new HashMap<>();  // empty map
    final Set<String> allImageTypes = new TreeSet<>();
    allImageTypes.add(AZKABAN_BASE_IMAGE);
    allImageTypes.add(AZKABAN_CONFIG);
    allImageTypes.addAll(jobTypes);
    final VersionSet versionSet = this.kubernetesContainerizedImpl
        .fetchVersionSet(flow.getExecutionId(), flowParam, allImageTypes, flow);
    final V1PodSpec podSpec = this.kubernetesContainerizedImpl
        .createPodSpec(flow.getExecutionId(), versionSet, jobTypes, flowParam);

    assert (podSpec != null);

    final V1Pod pod = this.kubernetesContainerizedImpl
        .createPodFromSpec(flow.getExecutionId(), podSpec);
    final String podSpecYaml = Yaml.dump(pod).trim();
    assert (!podSpecYaml.isEmpty());
    log.info("Pod spec for execution id {} is {}", flow.getExecutionId(), podSpecYaml);
  }

  @Test
  public void testVersionSetConstructionWithFlowOverrideParams() throws Exception {
    final ExecutableFlow flow = createFlowWithMultipleJobtypes();
    flow.setExecutionId(2);
    when(this.executorLoader.fetchExecutableFlow(flow.getExecutionId())).thenReturn(flow);
    when(imageRampupManager.getVersionByImageTypes(any(), any(Set.class)))
        .thenReturn(getVersionMap());
    when(imageRampupManager
        .getVersionInfoWithNewAndActiveState(any(String.class), any(String.class)))
        .thenReturn(new VersionInfo("7.0.4", "path1", State.ACTIVE));
    final TreeSet<String> jobTypes = ContainerImplUtils.getJobTypesForFlow(flow);
    // Add included job types
    jobTypes.add("hadoopJava");
    jobTypes.add("pig");
    jobTypes.add("pigLi-0.11.1");
    // Add azkaban base image and config
    final Set<String> allImageTypes = new TreeSet<>();
    allImageTypes.add(AZKABAN_BASE_IMAGE);
    allImageTypes.add(AZKABAN_CONFIG);
    allImageTypes.addAll(jobTypes);
    final VersionSetBuilder versionSetBuilder = new VersionSetBuilder(this.loader);
    final VersionSet presetVersionSet = versionSetBuilder
        .addElement("azkaban-base", new VersionInfo("7.0.4", "path1", State.ACTIVE))
        .addElement("azkaban-config", new VersionInfo("9.1.1", "path2", State.ACTIVE))
        .addElement("spark", new VersionInfo("8.0", "path3", State.ACTIVE))
        .addElement("kafkaPush", new VersionInfo("7.1", "path4", State.ACTIVE))
        .build();

    final Map<String, String> flowParam = new HashMap<>();
    flowParam.put(Constants.FlowParameters.FLOW_PARAM_VERSION_SET_ID,
        String.valueOf(presetVersionSet.getVersionSetId()));
    VersionSet versionSet = this.kubernetesContainerizedImpl
        .fetchVersionSet(flow.getExecutionId(), flowParam, allImageTypes, flow);

    assert (versionSet.getVersion("kafkaPush").get()
        .equals(presetVersionSet.getVersion("kafkaPush").get()));
    assert (versionSet.getVersion("spark").get()
        .equals(presetVersionSet.getVersion("spark").get()));
    assert (versionSet.getVersion("azkaban-base").get()
        .equals(presetVersionSet.getVersion("azkaban-base").get()));
    assert (versionSet.getVersion("azkaban-config").get()
        .equals(presetVersionSet.getVersion("azkaban-config").get()));
    // Included jobs in the azkaban base image, so must not present in versionSet.
    Assert.assertEquals(false, versionSet.getVersion("hadoopJava").isPresent());
    Assert.assertEquals(false, versionSet.getVersion("pig").isPresent());
    Assert.assertEquals(false, versionSet.getVersion("pigLi-0.11.1").isPresent());

    // Now let's try constructing an incomplete versionSet
    final VersionSetBuilder incompleteVersionSetBuilder = new VersionSetBuilder(this.loader);
    final VersionSet incompleteVersionSet = incompleteVersionSetBuilder
        .addElement("kafkaPush", new VersionInfo("7.1", "path1", State.ACTIVE))
        .addElement("spark", new VersionInfo("8.0", "path2", State.ACTIVE))
        .build();

    flowParam.put(Constants.FlowParameters.FLOW_PARAM_VERSION_SET_ID,
        String.valueOf(incompleteVersionSet.getVersionSetId()));
    flowParam.put(String.join(".",
        KubernetesContainerizedImpl.IMAGE, "azkaban-base", KubernetesContainerizedImpl.VERSION),
        "7.0.4");
    versionSet = this.kubernetesContainerizedImpl
        .fetchVersionSet(flow.getExecutionId(), flowParam, allImageTypes, flow);

    assert (versionSet.getVersion("kafkaPush").get()
        .equals(presetVersionSet.getVersion("kafkaPush").get()));
    assert (versionSet.getVersion("spark").get()
        .equals(presetVersionSet.getVersion("spark").get()));
    assert (versionSet.getVersion("azkaban-base").get().getVersion().equals("7.0.4"));
    assert (versionSet.getVersion("azkaban-config").get().getVersion().equals("9.1.1"));
  }

  @Test
  public void testVersionSetConstructionWithRampupManager() throws Exception {
    final ExecutableFlow flow = createFlowWithMultipleJobtypes();
    flow.setExecutionId(2);
    when(this.executorLoader.fetchExecutableFlow(flow.getExecutionId())).thenReturn(flow);
    when(imageRampupManager.getVersionByImageTypes(any(), any(Set.class)))
        .thenReturn(getVersionMap());
    final TreeSet<String> jobTypes = ContainerImplUtils.getJobTypesForFlow(flow);
    // Add included job types
    jobTypes.add("hadoopJava");
    jobTypes.add("pig");
    jobTypes.add("pigLi-0.11.1");
    jobTypes.add("noop");
    // Add azkaban base image and config
    final Set<String> allImageTypes = new TreeSet<>();
    allImageTypes.add(AZKABAN_BASE_IMAGE);
    allImageTypes.add(AZKABAN_CONFIG);
    allImageTypes.addAll(jobTypes);

    final Map<String, String> flowParam = new HashMap<>();

    final VersionSet versionSet = this.kubernetesContainerizedImpl
        .fetchVersionSet(flow.getExecutionId(), flowParam, allImageTypes, flow);

    // Included jobs in the azkaban base image, so must not present in versionSet.
    Assert.assertEquals(false, versionSet.getVersion("hadoopJava").isPresent());
    Assert.assertEquals(false, versionSet.getVersion("pig").isPresent());
    Assert.assertEquals(false, versionSet.getVersion("pigLi-0.11.1").isPresent());
    Assert.assertEquals(false, versionSet.getVersion("noop").isPresent());
    Assert.assertEquals("7.0.4", versionSet.getVersion("azkaban-base").get().getVersion());
    Assert.assertEquals("9.1.1", versionSet.getVersion("azkaban-config").get().getVersion());
    Assert.assertEquals("8.0", versionSet.getVersion("spark").get().getVersion());
    Assert.assertEquals("7.1", versionSet.getVersion("kafkaPush").get().getVersion());
  }

  @Test
  public void testDispatchingFlowEvent() throws Exception {
    final ExecutableFlow flow = createFlowWithMultipleJobtypes();
    flow.setExecutionId(2);
    when(this.executorLoader.fetchExecutableFlow(flow.getExecutionId())).thenReturn(flow);
    when(imageRampupManager.getVersionByImageTypes(any(), any(Set.class)))
        .thenReturn(getVersionMap());

    final TreeSet<String> jobTypes = ContainerImplUtils.getJobTypesForFlow(flow);

    final Map<String, String> flowParam = new HashMap<>();  // empty map
    final Set<String> allImageTypes = new TreeSet<>();
    allImageTypes.add(AZKABAN_BASE_IMAGE);
    allImageTypes.add(AZKABAN_CONFIG);
    allImageTypes.addAll(jobTypes);
    final VersionSet versionSet = this.kubernetesContainerizedImpl
        .fetchVersionSet(flow.getExecutionId(), flowParam, allImageTypes, flow);

    flow.setStatus(Status.PREPARING);
    flow.setVersionSet(versionSet);

    final PodEventListener podEventListener = new PodEventListener();

    SERVICE_PROVIDER.unsetInjector();
    SERVICE_PROVIDER.setInjector(getInjector(new Props()));
    final Map<String, String> metaData = podEventListener.getFlowMetaData(flow);

    Assert.assertTrue(metaData.get("executionId").equals("2"));
    Assert.assertTrue(metaData.get("flowStatus").equals("PREPARING"));
    final String versionSetJsonString = metaData.get("versionSet");
    final Map<String, String> imageToVersionMap =
        new ObjectMapper().readValue(versionSetJsonString,
        new TypeReference<HashMap<String, String>>() {
        });
    assertThat(imageToVersionMap.keySet()).isEqualTo(versionSet.getImageToVersionMap().keySet());
    assertThat(imageToVersionMap.get("spark")).isEqualTo(versionSet.getImageToVersionMap()
        .get("spark").getVersion());
    assertThat(imageToVersionMap.get(AZKABAN_BASE_IMAGE)).isEqualTo(versionSet.getImageToVersionMap()
        .get(AZKABAN_BASE_IMAGE).getVersion());
  }

  private ExecutableFlow createTestFlow() throws Exception {
    return TestUtils.createTestExecutableFlow("exectest1", "exec1", DispatchMethod.CONTAINERIZED);
  }

  private ExecutableFlow createFlowWithMultipleJobtypes() throws Exception {
    return TestUtils.createTestExecutableFlowFromYaml("embedded4", "valid_dag_2");
  }

  private static void setupImageTables() {
    imageTypeDao = new ImageTypeDaoImpl(dbOperator);
    imageVersionDao = new ImageVersionDaoImpl(dbOperator, imageTypeDao);
    imageRampupDao = new ImageRampupDaoImpl(dbOperator, imageTypeDao, imageVersionDao);
    // Create a mock of ImageRampupManager to get the image type and version map. This mock is
    // required as the completed flow of getting image type and version can't be tested by
    // populating image management table due non supported "UNSIGNED" integer in HSQL.
    imageRampupManager = mock(ImageRampupManagerImpl.class);
    imageTypeConverter = new ImageTypeConverter();
    imageVersionConverter = new ImageVersionConverter();
    imageRampupPlanConverter = new ImageRampupPlanConverter();
    final ObjectMapper objectMapper = new ObjectMapper();
    // Insert into all the below image tables is commented as in memory HSQL database does not
    // support "UNSIGNED" data type. Some of the queries in ImageVersionDaoImpl.java uses
    //"UNSIGNED" integer, all the insert entries are commented out and ImageRampupManager mock is
    //create above to get the image type and version map.
    /*addImageTypeTableEntry("image_type_hadoopJava.json", objectMapper);
    addImageTypeTableEntry("image_type_command.json", objectMapper);
    addImageTypeTableEntry("image_type_spark.json", objectMapper);
    addImageVersions("image_types_active_versions.json", objectMapper);
    addImageRampupEntries("create_image_rampup.json", objectMapper);*/
  }

  private static void addImageTypeTableEntry(final String jsonFile, final ObjectMapper objectMapper) {
    final String jsonPayload = JSONUtils.readJsonFileAsString(TEST_JSON_DIR + "/" + jsonFile);
    try {
      final ImageTypeDTO imageType = objectMapper.readValue(jsonPayload, ImageTypeDTO.class);
      imageType.setCreatedBy("azkaban");
      imageType.setModifiedBy("azkaban");
      imageTypeDao.createImageType(imageTypeConverter.convertToDataModel(imageType));
    } catch (final Exception e) {
      log.error("Failed to read from json file: " + jsonPayload);
      assert (false);
    }
  }

  private static void addImageVersions(final String jsonFile, final ObjectMapper objectMapper) {
    final String jsonPayload = JSONUtils.readJsonFileAsString(TEST_JSON_DIR + "/" + jsonFile);
    List<ImageVersionDTO> imageVersions = null;
    try {
      imageVersions = objectMapper.readValue(jsonPayload,
          new TypeReference<List<ImageVersionDTO>>() {
          });
      log.info(String.valueOf(imageVersions));
      for (final ImageVersionDTO imageVersion : imageVersions) {
        imageVersion.setCreatedBy("azkaban");
        imageVersion.setModifiedBy("azkaban");
        imageVersionDao.createImageVersion(imageVersionConverter.convertToDataModel(imageVersion));
      }
    } catch (final IOException e) {
      log.error("Exception while converting input json: ", e);
      assert (false);
    }
  }

  private static void addImageRampupEntries(final String jsonFile, final ObjectMapper objectMapper) {
    final String jsonPayload = JSONUtils.readJsonFileAsString(TEST_JSON_DIR + "/" + jsonFile);
    List<ImageRampupPlanRequestDTO> imageRampupPlanRequests = null;
    try {
      imageRampupPlanRequests = objectMapper.readValue(jsonPayload,
          new TypeReference<List<ImageRampupPlanRequestDTO>>() {
          });
      for (final ImageRampupPlanRequestDTO imageRampupPlanRequest : imageRampupPlanRequests) {
        imageRampupPlanRequest.setCreatedBy("azkaban");
        imageRampupPlanRequest.setModifiedBy("azkaban");
        imageRampupDao.createImageRampupPlan(
            imageRampupPlanConverter.convertToDataModel(imageRampupPlanRequest));
      }
    } catch (final IOException e) {
      log.error("Exception while converting input json: ", e);
      assert (false);
    }
  }

  private Map<String, VersionInfo> getVersionMap() {
    final Map<String, VersionInfo> versionMap = new TreeMap<>();
    versionMap.put(AZKABAN_BASE_IMAGE, new VersionInfo("7.0.4", "path1", State.ACTIVE));
    versionMap.put(AZKABAN_CONFIG, new VersionInfo("9.1.1", "path2", State.ACTIVE));
    versionMap.put("spark", new VersionInfo("8.0", "path3", State.ACTIVE));
    versionMap.put("kafkaPush", new VersionInfo("7.1", "path4", State.ACTIVE));
    return versionMap;
  }

  private Injector getInjector(final Props props){
    props.put(AZKABAN_EVENT_REPORTING_ENABLED, "true");
    props.put(AZKABAN_EVENT_REPORTING_CLASS_PARAM,
        "azkaban.project.AzkabanEventReporterTest");
    props.put("database.type", "h2");
    props.put("h2.path", "h2");
    return Guice.createInjector(new AzkabanCommonModule(props));
  }
}
