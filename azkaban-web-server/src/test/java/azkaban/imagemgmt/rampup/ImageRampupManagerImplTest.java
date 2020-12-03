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
package azkaban.imagemgmt.rampup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.imagemgmt.daos.ImageRampupDao;
import azkaban.imagemgmt.daos.ImageRampupDaoImpl;
import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.daos.ImageTypeDaoImpl;
import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.daos.ImageVersionDaoImpl;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageRampup;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.utils.JSONUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImageRampupManagerImplTest {

  private static final Logger log = LoggerFactory.getLogger(ImageRampupManagerImplTest.class);

  private ImageTypeDao imageTypeDao;
  private ImageVersionDao imageVersionDao;
  private ImageRampupDao imageRampupDao;
  private ObjectMapper objectMapper;
  private ImageRampupManger imageRampupManger;

  @Before
  public void setup() {
    objectMapper = new ObjectMapper();
    imageTypeDao = mock(ImageTypeDaoImpl.class);
    imageVersionDao = mock(ImageVersionDaoImpl.class);
    imageRampupDao = mock(ImageRampupDaoImpl.class);
    imageRampupManger = new ImageRampupManagerImpl(imageRampupDao, imageVersionDao, imageTypeDao);
  }

  /**
   * The test is for getting the specified image type version from the the active rampups.
   *
   * @throws Exception
   */
  @Test
  public void testFetchVersionByImageTypesCase1() throws Exception {
    String jsonInput = JSONUtils.readJsonFileAsString("image_management/image_type_rampups"
        + ".json");
    Map<String, List<ImageRampup>> imageTypeRampups = convertToRampupMap(jsonInput);
    Set<String> imageTypes = new TreeSet<>();
    imageTypes.add("spark_job");
    imageTypes.add("hive_job");
    imageTypes.add("azkaban_core");
    imageTypes.add("azkaban_config");
    imageTypes.add("azkaban_exec");
    when(imageRampupDao.fetchRampupByImageTypes(any(Set.class))).thenReturn(imageTypeRampups);
    Map<String, String> imageTypeVersionMap = imageRampupManger
        .fetchVersionByImageTypes(imageTypes);
    Assert.assertNotNull(imageTypeVersionMap);
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_config"));
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_core"));
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_exec"));
    Assert.assertNotNull(imageTypeVersionMap.get("hive_job"));
    Assert.assertNotNull(imageTypeVersionMap.get("spark_job"));
  }

  /**
   * This test is for getting the specified image types version from rampup as well as based on
   * active image version. The image types for which active rampup is present get the version from
   * rampups. For the remaining images it get the latest active version from image versions.
   *
   * @throws Exception
   */
  @Test
  public void testFetchVersionByImageTypesCase2() throws Exception {
    String jsonImageTypeRampups = JSONUtils.readJsonFileAsString("image_management/"
        + "image_type_rampups.json");
    Map<String, List<ImageRampup>> imageTypeRampups = convertToRampupMap(jsonImageTypeRampups);
    Set<String> imageTypes = new TreeSet<>();
    imageTypes.add("spark_job");
    imageTypes.add("hive_job");
    imageTypes.add("azkaban_core");
    imageTypes.add("azkaban_config");
    imageTypes.add("azkaban_exec");
    imageTypes.add("pig_job");
    imageTypes.add("hadoop_job");
    String jsonImageTypeActiveVersion = JSONUtils.readJsonFileAsString("image_management"
        + "/image_type_active_version.json");
    List<ImageVersion> activeImageVersions = convertToImageTypeVersionList(
        jsonImageTypeActiveVersion);
    when(imageRampupDao.fetchRampupByImageTypes(any(Set.class))).thenReturn(imageTypeRampups);
    when(imageVersionDao.getActiveVersionByImageTypes(any(Set.class)))
        .thenReturn(activeImageVersions);
    Map<String, String> imageTypeVersionMap = imageRampupManger
        .fetchVersionByImageTypes(imageTypes);
    Assert.assertNotNull(imageTypeVersionMap);
    // Below image type versions are obtained from active ramp up. Version is selected randomly
    // based on rampup percentage.
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_config"));
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_core"));
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_exec"));
    Assert.assertNotNull(imageTypeVersionMap.get("hive_job"));
    Assert.assertNotNull(imageTypeVersionMap.get("spark_job"));
    // Below two image types are from based on active image version
    Assert.assertNotNull(imageTypeVersionMap.get("pig_job"));
    Assert.assertEquals("4.1.2", imageTypeVersionMap.get("pig_job"));
    Assert.assertNotNull(imageTypeVersionMap.get("hadoop_job"));
    Assert.assertEquals("5.1.5", imageTypeVersionMap.get("hadoop_job"));
  }

  /**
   * For the given image types some of the versions are from active rampups, some of the versions
   * are based on active image version. But there are some image types for which there is neighter
   * active rampups nor active image version, hence throws exception.
   *
   * @throws Exception
   */
  @Test(expected = ImageMgmtException.class)
  public void testFetchVersionByImageTypesFailureCase() throws Exception {
    String jsonImageTypeRampups = JSONUtils.readJsonFileAsString("image_management/"
        + "image_type_rampups.json");
    Map<String, List<ImageRampup>> imageTypeRampups = convertToRampupMap(jsonImageTypeRampups);
    Set<String> imageTypes = new TreeSet<>();
    imageTypes.add("spark_job");
    imageTypes.add("hive_job");
    imageTypes.add("azkaban_core");
    imageTypes.add("azkaban_config");
    imageTypes.add("azkaban_exec");
    imageTypes.add("pig_job");
    imageTypes.add("hadoop_job");
    imageTypes.add("kabootar_job");
    imageTypes.add("wormhole_job");
    String jsonImageTypeActiveVersion = JSONUtils.readJsonFileAsString("image_management"
        + "/image_type_active_version.json");
    List<ImageVersion> activeImageVersions = convertToImageTypeVersionList(
        jsonImageTypeActiveVersion);
    when(imageRampupDao.fetchRampupByImageTypes(any(Set.class))).thenReturn(imageTypeRampups);
    when(imageVersionDao.getActiveVersionByImageTypes(any(Set.class)))
        .thenReturn(activeImageVersions);
    Map<String, String> imageTypeVersionMap = imageRampupManger
        .fetchVersionByImageTypes(imageTypes);
    Assert.assertNotNull(imageTypeVersionMap);
    // Below image type versions are obtained from active ramp up. Version is selected randomly
    // based on rampup percentage.
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_config"));
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_core"));
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_exec"));
    Assert.assertNotNull(imageTypeVersionMap.get("hive_job"));
    Assert.assertNotNull(imageTypeVersionMap.get("spark_job"));
    Assert.assertNotNull(imageTypeVersionMap.get("pig_job"));
    // Below two image types are from based on active image version
    Assert.assertEquals("4.1.2", imageTypeVersionMap.get("pig_job"));
    Assert.assertNotNull(imageTypeVersionMap.get("hadoop_job"));
    Assert.assertEquals("5.1.5", imageTypeVersionMap.get("hadoop_job"));
  }

  /**
   * This test is a success test for getting version for all the available image types. The versions
   * are either from active rampup or based on active image version.
   *
   * @throws Exception
   */
  @Test
  public void testFetchAllImageTypesVersion() throws Exception {
    String jsonImageTypeRampups = JSONUtils.readJsonFileAsString("image_management/"
        + "image_type_rampups.json");
    Map<String, List<ImageRampup>> imageTypeRampups = convertToRampupMap(jsonImageTypeRampups);
    String jsonAllImageTypes = JSONUtils.readJsonFileAsString("image_management/"
        + "all_image_types.json");
    List<ImageType> allImageTypes = convertToImageTypeList(jsonAllImageTypes);
    String jsonImageTypeActiveVersion = JSONUtils.readJsonFileAsString("image_management"
        + "/all_image_types_active_version.json");
    List<ImageVersion> activeImageVersions = convertToImageTypeVersionList(
        jsonImageTypeActiveVersion);
    when(imageRampupDao.fetchAllImageTypesRampup()).thenReturn(imageTypeRampups);
    when(imageTypeDao.getAllImageTypes()).thenReturn(allImageTypes);
    when(imageVersionDao.getActiveVersionByImageTypes(any(Set.class)))
        .thenReturn(activeImageVersions);
    Map<String, String> imageTypeVersionMap = imageRampupManger
        .fetchAllImageTypesVersion();
    Assert.assertNotNull(imageTypeVersionMap);
    // Below image type versions are obtained from active ramp up. Version is selected randomly
    // based on rampup percentage.
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_config"));
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_core"));
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_exec"));
    Assert.assertNotNull(imageTypeVersionMap.get("hive_job"));
    Assert.assertNotNull(imageTypeVersionMap.get("spark_job"));
    // Below two image types are from based on active image version
    Assert.assertNotNull(imageTypeVersionMap.get("pig_job"));
    Assert.assertEquals("4.1.2", imageTypeVersionMap.get("pig_job"));
    Assert.assertNotNull(imageTypeVersionMap.get("hadoop_job"));
    Assert.assertEquals("5.1.5", imageTypeVersionMap.get("hadoop_job"));
    Assert.assertNotNull(imageTypeVersionMap.get("kafka_push_job"));
    Assert.assertEquals("3.1.2", imageTypeVersionMap.get("kafka_push_job"));
    Assert.assertNotNull(imageTypeVersionMap.get("wormhole_job"));
    Assert.assertEquals("1.1.8", imageTypeVersionMap.get("wormhole_job"));
    Assert.assertNotNull(imageTypeVersionMap.get("kabootar_job"));
    Assert.assertEquals("1.1.4", imageTypeVersionMap.get("kabootar_job"));

  }

  /**
   * This test is a failure test for getting version for all the available image types. For some of
   * the image types there is neighter active image rampups nor active image versions. Hence the
   * test throws exception.
   *
   * @throws Exception
   */
  @Test(expected = ImageMgmtException.class)
  public void testFetchAllImageTypesVersionFailureCase() throws Exception {
    String jsonImageTypeRampups = JSONUtils.readJsonFileAsString("image_management/"
        + "image_type_rampups.json");
    Map<String, List<ImageRampup>> imageTypeRampups = convertToRampupMap(jsonImageTypeRampups);
    String jsonAllImageTypes = JSONUtils.readJsonFileAsString("image_management/"
        + "all_image_types.json");
    List<ImageType> allImageTypes = convertToImageTypeList(jsonAllImageTypes);
    String jsonImageTypeActiveVersion = JSONUtils.readJsonFileAsString("image_management"
        + "/image_type_active_version.json");
    List<ImageVersion> activeImageVersions = convertToImageTypeVersionList(
        jsonImageTypeActiveVersion);
    when(imageRampupDao.fetchAllImageTypesRampup()).thenReturn(imageTypeRampups);
    when(imageTypeDao.getAllImageTypes()).thenReturn(allImageTypes);
    when(imageVersionDao.getActiveVersionByImageTypes(any(Set.class)))
        .thenReturn(activeImageVersions);
    Map<String, String> imageTypeVersionMap = imageRampupManger
        .fetchAllImageTypesVersion();
    Assert.assertNotNull(imageTypeVersionMap);
    // Below image type versions are obtained from active ramp up. Version is selected randomly
    // based on rampup percentage.
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_config"));
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_core"));
    Assert.assertNotNull(imageTypeVersionMap.get("azkaban_exec"));
    Assert.assertNotNull(imageTypeVersionMap.get("hive_job"));
    Assert.assertNotNull(imageTypeVersionMap.get("spark_job"));
    Assert.assertNotNull(imageTypeVersionMap.get("pig_job"));
    // Below two image types are from based on active image version
    Assert.assertEquals("4.1.2", imageTypeVersionMap.get("pig_job"));
    Assert.assertNotNull(imageTypeVersionMap.get("hadoop_job"));
    Assert.assertEquals("5.1.5", imageTypeVersionMap.get("hadoop_job"));
  }

  private Map<String, List<ImageRampup>> convertToRampupMap(String input) {
    Map<String, List<ImageRampup>> imageTypeRampups = null;
    try {
      imageTypeRampups = objectMapper.readValue(input,
          new TypeReference<Map<String,
              List<ImageRampup>>>() {
          });
    } catch (IOException e) {
      log.error("Exception while converting input json ", e);
    }
    return imageTypeRampups;
  }

  private List<ImageVersion> convertToImageTypeVersionList(String input) {
    List<ImageVersion> imageVersions = null;
    try {
      imageVersions = objectMapper.readValue(input,
          new TypeReference<List<ImageVersion>>() {
          });
    } catch (IOException e) {
      log.error("Exception while converting input json ", e);
    }
    return imageVersions;
  }

  private List<ImageType> convertToImageTypeList(String input) {
    List<ImageType> imageTypes = null;
    try {
      imageTypes = objectMapper.readValue(input,
          new TypeReference<List<ImageType>>() {
          });
    } catch (IOException e) {
      log.error("Exception while converting input json ", e);
    }
    return imageTypes;
  }
}
