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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.db.DatabaseOperator;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.Status;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.daos.ImageRampupDao;
import azkaban.imagemgmt.daos.ImageRampupDaoImpl;
import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.daos.ImageTypeDaoImpl;
import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.daos.ImageVersionDaoImpl;
import azkaban.imagemgmt.models.ImageRampupPlanRequest;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.rampup.ImageRampupManager;
import azkaban.imagemgmt.rampup.ImageRampupManagerImpl;
import azkaban.imagemgmt.version.VersionSet;
import azkaban.imagemgmt.version.VersionSetBuilder;
import azkaban.imagemgmt.version.VersionSetLoader;
import azkaban.imagemgmt.version.JdbcVersionSetLoader;
import azkaban.test.Utils;
import azkaban.utils.JSONUtils;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.util.Yaml;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import azkaban.utils.Props;
import azkaban.utils.TestUtils;
import java.util.TreeSet;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

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
    this.executorLoader = mock(ExecutorLoader.class);
    this.loader = new JdbcVersionSetLoader(this.dbOperator);
    this.kubernetesContainerizedImpl = new KubernetesContainerizedImpl(this.props,
        this.executorLoader, this.loader, this.imageRampupManager);
  }

  @Test
  public void testJobTypesInFlow() throws Exception {
    final ExecutableFlow flow = createTestFlow();
    flow.setSubmitUser("testUser1");
    flow.setStatus(Status.PREPARING);
    flow.setSubmitTime(System.currentTimeMillis());
    flow.setExecutionId(0);
    TreeSet<String> jobTypes = this.kubernetesContainerizedImpl.getJobTypesForFlow(flow);
    assertThat(jobTypes.size()).isEqualTo(1);
  }

  @Test
  public void testPodConstruction() throws Exception {
    final ExecutableFlow flow = createFlowWithMultipleJobtypes();
    flow.setExecutionId(1);
    when(this.executorLoader.fetchExecutableFlow(flow.getExecutionId())).thenReturn(flow);
    final TreeSet<String> jobTypes = this.kubernetesContainerizedImpl.getJobTypesForFlow(flow);
    assert(jobTypes.contains("command"));
    assert(jobTypes.contains("hadoopJava"));
    assert(jobTypes.contains("spark"));
    log.info("Jobtypes for flow {} are: {}", flow.getFlowId(), jobTypes);

    final Map<String, String> flowParam = new HashMap<>();  // empty map

    VersionSet versionSet = this.kubernetesContainerizedImpl
        .fetchVersionSet(flow.getExecutionId(), flowParam, jobTypes);
    final V1PodSpec podSpec = this.kubernetesContainerizedImpl
        .createPodSpec(flow.getExecutionId(), versionSet, jobTypes);

    assert(podSpec != null);

    final V1Pod pod = this.kubernetesContainerizedImpl.createPodFromSpec(flow.getExecutionId(), podSpec);
    String podSpecYaml = Yaml.dump(pod).trim();
    assert(!podSpecYaml.isEmpty());
    log.info("Pod spec for execution id {} is {}", flow.getExecutionId(), podSpecYaml);
  }

  @Test
  public void testVersionSetConstructionWithFlowOverrideParams() throws Exception {
    final ExecutableFlow flow = createFlowWithMultipleJobtypes();
    flow.setExecutionId(2);
    when(this.executorLoader.fetchExecutableFlow(flow.getExecutionId())).thenReturn(flow);
    final TreeSet<String> jobTypes = this.kubernetesContainerizedImpl.getJobTypesForFlow(flow);
    VersionSetBuilder versionSetBuilder = new VersionSetBuilder(this.loader);
    VersionSet presetVersionSet = versionSetBuilder
        .addElement("command", "7.1")
        .addElement("spark", "8.0")
        .addElement("hadoopJava", "7.0.4")
        .build();

    final Map<String, String> flowParam = new HashMap<>();
    flowParam.put(KubernetesContainerizedImpl.FLOW_PARAM_VERSION_SET_ID,
        String.valueOf(presetVersionSet.getVersionSetId()));
    VersionSet versionSet = this.kubernetesContainerizedImpl
        .fetchVersionSet(flow.getExecutionId(), flowParam, jobTypes);

    assert(versionSet.getVersion("command").get()
        .equals(presetVersionSet.getVersion("command").get()));
    assert(versionSet.getVersion("spark").get()
        .equals(presetVersionSet.getVersion("spark").get()));
    assert(versionSet.getVersion("hadoopJava").get()
        .equals(presetVersionSet.getVersion("hadoopJava").get()));

    // Now let's try constructing an incomplete versionSet
    VersionSetBuilder incompleteVersionSetBuilder = new VersionSetBuilder(this.loader);
    VersionSet incompleteVersionSet = incompleteVersionSetBuilder
        .addElement("command", "7.1")
        .addElement("spark", "8.0")
        .addElement("pig", "1.2")
        .build();

    flowParam.put(KubernetesContainerizedImpl.FLOW_PARAM_VERSION_SET_ID,
       String.valueOf(incompleteVersionSet.getVersionSetId()));
    flowParam.put(String.join(".",
        KubernetesContainerizedImpl.IMAGE, "hadoopJava", KubernetesContainerizedImpl.VERSION),
        "10.0.2");
    versionSet = this.kubernetesContainerizedImpl
        .fetchVersionSet(flow.getExecutionId(), flowParam, jobTypes);

    assert(versionSet.getVersion("command").get()
        .equals(presetVersionSet.getVersion("command").get()));
    assert(versionSet.getVersion("spark").get()
        .equals(presetVersionSet.getVersion("spark").get()));
    assert(versionSet.getVersion("hadoopJava").get().equals("10.0.2"));
  }

  private ExecutableFlow createTestFlow() throws Exception {
    return TestUtils.createTestExecutableFlow("exectest1", "exec1");
  }

  private ExecutableFlow createFlowWithMultipleJobtypes() throws Exception {
    return TestUtils.createTestExecutableFlowFromYaml("embedded4", "valid_dag_2");
  }

  private static void setupImageTables() {
    imageTypeDao = new ImageTypeDaoImpl(dbOperator);
    imageVersionDao = new ImageVersionDaoImpl(dbOperator, imageTypeDao);
    imageRampupDao = new ImageRampupDaoImpl(dbOperator, imageTypeDao, imageVersionDao);
    imageRampupManager = new ImageRampupManagerImpl(imageRampupDao, imageVersionDao,
        imageTypeDao);
    final ObjectMapper objectMapper = new ObjectMapper();
    addImageTypeTableEntry("image_type_hadoopJava.json", objectMapper);
    addImageTypeTableEntry("image_type_command.json", objectMapper);
    addImageTypeTableEntry("image_type_spark.json", objectMapper);
    addImageVersions("image_types_active_versions.json", objectMapper);
    addImageRampupEntries("create_image_rampup.json", objectMapper);
  }

  private static void addImageTypeTableEntry(String jsonFile, ObjectMapper objectMapper) {
    String jsonPayload = JSONUtils.readJsonFileAsString(TEST_JSON_DIR + "/" + jsonFile);
    try {
      ImageType imageType = objectMapper.readValue(jsonPayload, ImageType.class);
      imageType.setCreatedBy("azkaban");
      imageType.setModifiedBy("azkaban");
      imageTypeDao.createImageType(imageType);
    } catch (Exception e) {
      log.error("Failed to read from json file: " + jsonPayload);
      assert (false);
    }
  }

  private static void addImageVersions(String jsonFile, ObjectMapper objectMapper) {
    String jsonPayload = JSONUtils.readJsonFileAsString(TEST_JSON_DIR + "/" + jsonFile);
    List<ImageVersion> imageVersions = null;
    try {
      imageVersions = objectMapper.readValue(jsonPayload,
          new TypeReference<List<ImageVersion>>() { });
      log.info(String.valueOf(imageVersions));
      for (ImageVersion imageVersion : imageVersions) {
        imageVersion.setCreatedBy("azkaban");
        imageVersion.setModifiedBy("azkaban");
        imageVersionDao.createImageVersion(imageVersion);
      }
    } catch (IOException e) {
      log.error("Exception while converting input json: ", e);
      assert (false);
    }
  }

  private static void addImageRampupEntries(String jsonFile, ObjectMapper objectMapper) {
    String jsonPayload = JSONUtils.readJsonFileAsString(TEST_JSON_DIR + "/" + jsonFile);
    List<ImageRampupPlanRequest> imageRampupPlanRequests = null;
    try {
      imageRampupPlanRequests = objectMapper.readValue(jsonPayload,
          new TypeReference<List<ImageRampupPlanRequest>>() { });
      for (ImageRampupPlanRequest imageRampupPlanRequest : imageRampupPlanRequests) {
        imageRampupPlanRequest.setCreatedBy("azkaban");
        imageRampupPlanRequest.setModifiedBy("azkaban");
        imageRampupDao.createImageRampupPlan(imageRampupPlanRequest);
      }
    } catch (IOException e) {
      log.error("Exception while converting input json: ", e);
      assert (false);
    }
  }
}