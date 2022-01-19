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
package azkaban.imagemgmt.services;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.imagemgmt.converters.Converter;
import azkaban.imagemgmt.converters.ImageTypeConverter;
import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.daos.ImageTypeDaoImpl;
import azkaban.imagemgmt.dto.ImageOwnershipDTO;
import azkaban.imagemgmt.dto.ImageTypeDTO;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.exception.ImageMgmtInvalidInputException;
import azkaban.imagemgmt.exception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageOwnership;
import azkaban.imagemgmt.models.ImageOwnership.Role;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.models.ImageType.Deployable;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.utils.JSONUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ImageTypeServiceImplTest {

  private ImageTypeDao imageTypeDao;
  private ObjectMapper objectMapper;
  private ImageTypeService imageTypeService;
  private ConverterUtils converterUtils;
  private Converter<ImageTypeDTO, ImageTypeDTO, ImageType> converter;

  @Before
  public void setup() {
    this.objectMapper = new ObjectMapper();
    this.imageTypeDao = mock(ImageTypeDaoImpl.class);
    this.converterUtils = new ConverterUtils(this.objectMapper);
    this.converter = new ImageTypeConverter();
    this.imageTypeService = new ImageTypeServiceImpl(this.imageTypeDao, this.converter);
  }

  @Test
  public void testCreateImageType() throws Exception {
    final String jsonPayload = JSONUtils.readJsonFileAsString("image_management/image_type.json");
    final ImageTypeDTO imageTypeDTO = converterUtils.convertToDTO(jsonPayload, ImageTypeDTO.class);
    imageTypeDTO.setCreatedBy("azkaban");
    imageTypeDTO.setModifiedBy("azkaban");
    when(this.imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    final int imageTypeId = this.imageTypeService.createImageType(imageTypeDTO);
    final ArgumentCaptor<ImageType> imageTypeArgumentCaptor =
        ArgumentCaptor.forClass(ImageType.class);
    verify(this.imageTypeDao, times(1)).createImageType(imageTypeArgumentCaptor.capture());
    final ImageType capturedImageType = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("kafka_push_job", capturedImageType.getName());
    Assert.assertEquals("azkaban", capturedImageType.getCreatedBy());
    Assert.assertEquals("image", capturedImageType.getDeployable().getName());
    Assert.assertNotNull(capturedImageType.getOwnerships());
    Assert.assertEquals(2, capturedImageType.getOwnerships().size());
    Assert.assertEquals(100, imageTypeId);
  }

  @Test
  public void testAddOwnerToImageType() throws Exception {
    final String jsonPayload = JSONUtils.readJsonFileAsString("image_management/image_type.json");
    final ImageTypeDTO imageTypeDTO = converterUtils.convertToDTO(jsonPayload, ImageTypeDTO.class);
    imageTypeDTO.setCreatedBy("azkaban");
    imageTypeDTO.setModifiedBy("azkaban");
    when(this.imageTypeDao.addOwnerOfImageType(any(ImageType.class))).thenReturn(200);
    final int imageTypeId = this.imageTypeService.updateImageType(imageTypeDTO, "addImageOwners");
    final ArgumentCaptor<ImageType> imageTypeArgumentCaptor =
        ArgumentCaptor.forClass(ImageType.class);
    verify(this.imageTypeDao, times(1)).addOwnerOfImageType(imageTypeArgumentCaptor.capture());
    final ImageType capturedImageType = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("kafka_push_job", capturedImageType.getName());
    Assert.assertEquals("azkaban", capturedImageType.getCreatedBy());
    Assert.assertEquals("image", capturedImageType.getDeployable().getName());
    Assert.assertNotNull(capturedImageType.getOwnerships());
    Assert.assertEquals(2, capturedImageType.getOwnerships().size());
    Assert.assertEquals(200, imageTypeId);
  }

  @Test
  public void testRemoveOwnerFromImageType() throws Exception {
    final String jsonPayload = JSONUtils.readJsonFileAsString("image_management/image_type.json");
    final ImageTypeDTO imageTypeDTO = converterUtils.convertToDTO(jsonPayload, ImageTypeDTO.class);
    imageTypeDTO.setCreatedBy("azkaban");
    imageTypeDTO.setModifiedBy("azkaban");
    when(this.imageTypeDao.removeOwnerOfImageType(any(ImageType.class))).thenReturn(300);
    final int imageTypeId = this.imageTypeService.updateImageType(imageTypeDTO,
        "removeImageOwners");
    final ArgumentCaptor<ImageType> imageTypeArgumentCaptor =
        ArgumentCaptor.forClass(ImageType.class);
    verify(this.imageTypeDao, times(1)).removeOwnerOfImageType(imageTypeArgumentCaptor.capture());
    final ImageType capturedImageType = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("kafka_push_job", capturedImageType.getName());
    Assert.assertEquals("azkaban", capturedImageType.getCreatedBy());
    Assert.assertEquals("image", capturedImageType.getDeployable().getName());
    Assert.assertNotNull(capturedImageType.getOwnerships());
    Assert.assertEquals(2, capturedImageType.getOwnerships().size());
    Assert.assertEquals(300, imageTypeId);
  }

  @Test
  public void testGetAllImageTypesWithOwnerships() throws Exception {
    List<ImageType> imageTypes = getImageTypeList();
    ImageTypeDTO expected = getImageTypeDTO();
    when(this.imageTypeDao.getAllImageTypesWithOwnerships()).thenReturn(imageTypes);
    List<ImageTypeDTO> imageTypeDTOs = this.imageTypeService.getAllImageTypesWithOwnerships();
    assert imageTypeDTOs.size() == 1;
    assertObjectsAreEqual(imageTypeDTOs.get(0), expected);
  }

  @Test
  public void testFindImageTypeWithOwnershipsById() throws Exception {
    final String id = "1";
    ImageTypeDTO expected = getImageTypeDTO();
    ImageType imageType = getImageType();
    when(this.imageTypeDao.getImageTypeWithOwnershipsById(id)).thenReturn(imageType);
    ImageTypeDTO given = this.imageTypeService.findImageTypeWithOwnershipsById(id);
    assertObjectsAreEqual(given, expected);
  }

  @Test
  public void testFindImageTypeWithOwnersByName() throws Exception {
    final ImageType imageType = getImageType();
    ImageTypeDTO expected = getImageTypeDTO();
    when(this.imageTypeDao.getImageTypeWithOwnershipsByName(any(String.class))).thenReturn
        ((Optional<ImageType>) Optional.of(imageType));
    ImageTypeDTO given = this.imageTypeService.findImageTypeWithOwnershipsByName(
        "anyString");
    assertObjectsAreEqual(given, expected);
  }

  @Test(expected = ImageMgmtException.class)
  public void testFindImageTypeWithOwnersByNameFailsWithImageMgmtException() throws Exception {
    when(this.imageTypeDao.getImageTypeWithOwnershipsByName(any(String.class)))
        .thenReturn(Optional.empty());
    this.imageTypeService.findImageTypeWithOwnershipsByName("anyString");
  }

  @Test
  public void testCreateImageTypeForConfigs() throws Exception {
    final String jsonPayload = JSONUtils
        .readJsonFileAsString("image_management/image_type_configs.json");
    final ImageTypeDTO imageTypeDTO = converterUtils.convertToDTO(jsonPayload, ImageTypeDTO.class);
    imageTypeDTO.setCreatedBy("azkaban");
    imageTypeDTO.setModifiedBy("azkaban");
    when(this.imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    final int imageTypeId = this.imageTypeService.createImageType(imageTypeDTO);
    final ArgumentCaptor<ImageType> imageTypeArgumentCaptor =
        ArgumentCaptor.forClass(ImageType.class);
    verify(this.imageTypeDao, times(1)).createImageType(imageTypeArgumentCaptor.capture());
    final ImageType capturedImageType = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("configs", capturedImageType.getName());
    Assert.assertEquals("azkaban", capturedImageType.getCreatedBy());
    Assert.assertEquals("tar", capturedImageType.getDeployable().getName());
    Assert.assertEquals(100, imageTypeId);
  }

  @Test(expected = ImageMgmtValidationException.class)
  public void testCreateImageTypeInvalidType() throws IOException {
    final String jsonPayload = JSONUtils
        .readJsonFileAsString("image_management/invalid_image_type.json");
    final ImageTypeDTO imageTypeDTO = converterUtils.convertToDTO(jsonPayload, ImageTypeDTO.class);
    imageTypeDTO.setCreatedBy("azkaban");
    imageTypeDTO.setModifiedBy("azkaban");
    when(this.imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    this.imageTypeService.createImageType(imageTypeDTO);

  }

  @Test(expected = ImageMgmtInvalidInputException.class)
  public void testCreateImageTypeInvalidDeployable() throws IOException {
    final String jsonPayload = JSONUtils
        .readJsonFileAsString("image_management/create_image_type_invalid_deployable.json");
    final ImageTypeDTO imageTypeDTO = converterUtils.convertToDTO(jsonPayload, ImageTypeDTO.class);
    imageTypeDTO.setCreatedBy("azkaban");
    imageTypeDTO.setModifiedBy("azkaban");
    when(this.imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    this.imageTypeService.createImageType(imageTypeDTO);

  }

  @Test(expected = ImageMgmtInvalidInputException.class)
  public void testCreateImageTypeInvalidRole() throws IOException {
    final String jsonPayload = JSONUtils
        .readJsonFileAsString("image_management/create_image_type_invalid_role.json");
    final ImageTypeDTO imageTypeDTO = converterUtils.convertToDTO(jsonPayload, ImageTypeDTO.class);
    imageTypeDTO.setCreatedBy("azkaban");
    imageTypeDTO.setModifiedBy("azkaban");
    when(this.imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    this.imageTypeService.createImageType(imageTypeDTO);

  }

  private void assertObjectsAreEqual(ImageTypeDTO given, ImageTypeDTO expected) {
    Assert.assertEquals(given.getName(), expected.getName());
    Assert.assertEquals(given.getDescription(), expected.getDescription());
    Assert.assertEquals(given.getDeployable(), expected.getDeployable());
    Assert.assertEquals(given.getOwnerships().size(), expected.getOwnerships().size());
  }

  private List<ImageType> getImageTypeList() {
    List<ImageType> imageTypes = new ArrayList<>();
    ImageType imageType = getImageType();
    imageTypes.add(imageType);
    return imageTypes;
  }

  private ImageType getImageType() {
    ImageType imageType = new ImageType();
    imageType.setName("name");
    imageType.setDeployable(Deployable.IMAGE);
    imageType.setDescription("description");
    ImageOwnership imageOwnership = new ImageOwnership();
    imageOwnership.setOwner("owner");
    imageOwnership.setName("name");
    imageOwnership.setRole(Role.ADMIN);
    List<ImageOwnership> imageOwnerships = new ArrayList<>();
    imageOwnerships.add(imageOwnership);
    imageType.setOwnerships(imageOwnerships);
    return imageType;
  }

  private ImageTypeDTO getImageTypeDTO() {
    ImageTypeDTO imageTypeDTO = new ImageTypeDTO();
    imageTypeDTO.setName("name");
    imageTypeDTO.setDeployable(Deployable.IMAGE);
    imageTypeDTO.setDescription("description");
    ImageOwnershipDTO imageOwnershipDTO = new ImageOwnershipDTO();
    imageOwnershipDTO.setOwner("owner");
    imageOwnershipDTO.setName("name");
    imageOwnershipDTO.setRole(Role.ADMIN);
    List<ImageOwnershipDTO> imageOwnershipsDTO = new ArrayList<>();
    imageOwnershipsDTO.add(imageOwnershipDTO);
    imageTypeDTO.setOwnerships(imageOwnershipsDTO);
    return imageTypeDTO;
  }
}
