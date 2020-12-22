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

import azkaban.imagemgmt.converters.Converter;
import azkaban.imagemgmt.converters.ImageTypeConverter;
import azkaban.imagemgmt.converters.ImageVersionConverter;
import azkaban.imagemgmt.daos.ImageRampupDao;
import azkaban.imagemgmt.daos.ImageRampupDaoImpl;
import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.daos.ImageTypeDaoImpl;
import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.daos.ImageVersionDaoImpl;
import azkaban.imagemgmt.dto.ImageTypeDTO;
import azkaban.imagemgmt.dto.ImageVersionDTO;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageRampup;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.utils.JSONUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
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
  private ImageRampupManager imageRampupManger;
  private Converter<ImageTypeDTO, ImageTypeDTO, ImageType> imageTypeConverter;
  private Converter<ImageVersionDTO, ImageVersionDTO, ImageVersion> imageVersionConverter;
  private ConverterUtils converterUtils;

  @Before
  public void setup() {
    this.objectMapper = new ObjectMapper();
    this.imageTypeDao = mock(ImageTypeDaoImpl.class);
    this.imageVersionDao = mock(ImageVersionDaoImpl.class);
    this.imageRampupDao = mock(ImageRampupDaoImpl.class);
    this.imageTypeConverter = new ImageTypeConverter();
    this.imageVersionConverter = new ImageVersionConverter();
    this.converterUtils = new ConverterUtils(this.objectMapper);
    this.imageRampupManger = new ImageRampupManagerImpl(this.imageRampupDao, this.imageVersionDao,
        this.imageTypeDao);
  }

  /**
   * The test is for getting the specified image type version from the the active rampups.
   *
   * @throws Exception
   */
  @Test
  public void testFetchVersionByImageTypesCase1() throws Exception {
    final String jsonInput = JSONUtils.readJsonFileAsString("image_management/image_type_rampups"
        + ".json");
    final Map<String, List<ImageRampup>> imageTypeRampups = convertToRampupMap(jsonInput);
    final Set<String> imageTypes = new TreeSet<>();
    imageTypes.add("spark_job");
    imageTypes.add("hive_job");
    imageTypes.add("azkaban_core");
    imageTypes.add("azkaban_config");
    imageTypes.add("azkaban_exec");
    when(this.imageRampupDao.getRampupByImageTypes(any(Set.class))).thenReturn(imageTypeRampups);
    final Map<String, String> imageTypeVersionMap = this.imageRampupManger
        .getVersionByImageTypes(imageTypes);
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
    final String jsonImageTypeRampups = JSONUtils.readJsonFileAsString("image_management/"
        + "image_type_rampups.json");
    final Map<String, List<ImageRampup>> imageTypeRampups = convertToRampupMap(jsonImageTypeRampups);
    final Set<String> imageTypes = new TreeSet<>();
    imageTypes.add("spark_job");
    imageTypes.add("hive_job");
    imageTypes.add("azkaban_core");
    imageTypes.add("azkaban_config");
    imageTypes.add("azkaban_exec");
    imageTypes.add("pig_job");
    imageTypes.add("hadoop_job");
    final String jsonImageTypeActiveVersion = JSONUtils.readJsonFileAsString("image_management"
        + "/image_type_active_version.json");
    final List<ImageVersionDTO> activeImageVersionDTOs = converterUtils.convertToDTOs(
        jsonImageTypeActiveVersion, ImageVersionDTO.class);
    final List<ImageVersion> activeImageVersions =
        this.imageVersionConverter.convertToDataModels(activeImageVersionDTOs);
    when(this.imageRampupDao.getRampupByImageTypes(any(Set.class))).thenReturn(imageTypeRampups);
    when(this.imageVersionDao.getActiveVersionByImageTypes(any(Set.class)))
        .thenReturn(activeImageVersions);
    final Map<String, String> imageTypeVersionMap = this.imageRampupManger
        .getVersionByImageTypes(imageTypes);
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
    final String jsonImageTypeRampups = JSONUtils.readJsonFileAsString("image_management/"
        + "image_type_rampups.json");
    final Map<String, List<ImageRampup>> imageTypeRampups = convertToRampupMap(jsonImageTypeRampups);
    final Set<String> imageTypes = new TreeSet<>();
    imageTypes.add("spark_job");
    imageTypes.add("hive_job");
    imageTypes.add("azkaban_core");
    imageTypes.add("azkaban_config");
    imageTypes.add("azkaban_exec");
    imageTypes.add("pig_job");
    imageTypes.add("hadoop_job");
    imageTypes.add("kabootar_job");
    imageTypes.add("wormhole_job");
    final String jsonImageTypeActiveVersion = JSONUtils.readJsonFileAsString("image_management"
        + "/image_type_active_version.json");
    final List<ImageVersionDTO> activeImageVersionDTOs = converterUtils.convertToDTOs(
        jsonImageTypeActiveVersion, ImageVersionDTO.class);
    final List<ImageVersion> activeImageVersions =
        this.imageVersionConverter.convertToDataModels(activeImageVersionDTOs);
    when(this.imageRampupDao.getRampupByImageTypes(any(Set.class))).thenReturn(imageTypeRampups);
    when(this.imageVersionDao.getActiveVersionByImageTypes(any(Set.class)))
        .thenReturn(activeImageVersions);
    final Map<String, String> imageTypeVersionMap = this.imageRampupManger
        .getVersionByImageTypes(imageTypes);
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
    final String jsonImageTypeRampups = JSONUtils.readJsonFileAsString("image_management/"
        + "image_type_rampups.json");
    final Map<String, List<ImageRampup>> imageTypeRampups = convertToRampupMap(jsonImageTypeRampups);
    final String jsonAllImageTypes = JSONUtils.readJsonFileAsString("image_management/"
        + "all_image_types.json");
    final List<ImageTypeDTO> allImageTypeDTOs = converterUtils.convertToDTOs(jsonAllImageTypes,
        ImageTypeDTO.class);
    final List<ImageType> allImageTypes =
        this.imageTypeConverter.convertToDataModels(allImageTypeDTOs);
    final String jsonImageTypeActiveVersion = JSONUtils.readJsonFileAsString("image_management"
        + "/all_image_types_active_version.json");
    final List<ImageVersionDTO> activeImageVersionDTOs = converterUtils.convertToDTOs(
        jsonImageTypeActiveVersion, ImageVersionDTO.class);
    final List<ImageVersion> activeImageVersions =
        this.imageVersionConverter.convertToDataModels(activeImageVersionDTOs);
    when(this.imageRampupDao.getRampupForAllImageTypes()).thenReturn(imageTypeRampups);
    when(this.imageTypeDao.getAllImageTypes()).thenReturn(allImageTypes);
    when(this.imageVersionDao.getActiveVersionByImageTypes(any(Set.class)))
        .thenReturn(activeImageVersions);
    final Map<String, String> imageTypeVersionMap = this.imageRampupManger
        .getVersionForAllImageTypes();
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
    final String jsonImageTypeRampups = JSONUtils.readJsonFileAsString("image_management/"
        + "image_type_rampups.json");
    final Map<String, List<ImageRampup>> imageTypeRampups = convertToRampupMap(jsonImageTypeRampups);
    final String jsonAllImageTypes = JSONUtils.readJsonFileAsString("image_management/"
        + "all_image_types.json");
    final List<ImageTypeDTO> allImageTypeDTOs = converterUtils.convertToDTOs(jsonAllImageTypes,
        ImageTypeDTO.class);
    final List<ImageType> allImageTypes =
        this.imageTypeConverter.convertToDataModels(allImageTypeDTOs);
    final String jsonImageTypeActiveVersion = JSONUtils.readJsonFileAsString("image_management"
        + "/image_type_active_version.json");
    final List<ImageVersionDTO> activeImageVersionDTOs = converterUtils.convertToDTOs(
        jsonImageTypeActiveVersion, ImageVersionDTO.class);
    final List<ImageVersion> activeImageVersions =
        this.imageVersionConverter.convertToDataModels(activeImageVersionDTOs);
    when(this.imageRampupDao.getRampupForAllImageTypes()).thenReturn(imageTypeRampups);
    when(this.imageTypeDao.getAllImageTypes()).thenReturn(allImageTypes);
    when(this.imageVersionDao.getActiveVersionByImageTypes(any(Set.class)))
        .thenReturn(activeImageVersions);
    final Map<String, String> imageTypeVersionMap = this.imageRampupManger
        .getVersionForAllImageTypes();
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

  private Map<String, List<ImageRampup>> convertToRampupMap(final String input) {
    Map<String, List<ImageRampup>> imageTypeRampups = null;
    try {
      imageTypeRampups = this.objectMapper.readValue(input,
          new TypeReference<Map<String,
              List<ImageRampup>>>() {
          });
    } catch (final IOException e) {
      log.error("Exception while converting input json ", e);
    }
    return imageTypeRampups;
  }
}
