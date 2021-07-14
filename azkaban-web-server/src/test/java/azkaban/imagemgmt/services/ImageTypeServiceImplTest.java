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
import azkaban.imagemgmt.dto.ImageTypeDTO;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.exception.ImageMgmtInvalidInputException;
import azkaban.imagemgmt.exception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.utils.JSONUtils;
import java.io.IOException;
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
  public void testFindImageTypeWithOwnersByName() throws Exception {
    final ImageType imageType = new ImageType();
    when(this.imageTypeDao.getImageTypeWithOwnershipsByName(any(String.class))).thenReturn
        ((Optional<ImageType>) Optional.of(imageType));
    this.imageTypeService.findImageTypeWithOwnersByName("anyString");
  }

  @Test(expected = ImageMgmtException.class)
  public void testFindImageTypeWithOwnersByNameFailsWithImageMgmtException() throws Exception {
    when(this.imageTypeDao.getImageTypeWithOwnershipsByName(any(String.class)))
        .thenReturn(Optional.empty());
    this.imageTypeService.findImageTypeWithOwnersByName("anyString");
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

}
