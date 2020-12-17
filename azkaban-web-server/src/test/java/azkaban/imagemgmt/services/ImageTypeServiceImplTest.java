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

import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.daos.ImageTypeDaoImpl;
import azkaban.imagemgmt.dto.ImageMetadataRequest;
import azkaban.imagemgmt.exeception.ImageMgmtInvalidInputException;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.utils.JSONUtils;
import java.io.IOException;
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

  @Before
  public void setup() {
    this.objectMapper = new ObjectMapper();
    this.imageTypeDao = mock(ImageTypeDaoImpl.class);
    this.converterUtils = new ConverterUtils(this.objectMapper);
    this.imageTypeService = new ImageTypeServiceImpl(this.imageTypeDao, this.converterUtils);
  }

  @Test
  public void testCreateImageType() throws Exception {
    final String jsonPayload = JSONUtils.readJsonFileAsString("image_management/image_type.json");
    final ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(this.imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    final int imageTypeId = this.imageTypeService.createImageType(imageMetadataRequest);
    final ArgumentCaptor<ImageType> imageTypeArgumentCaptor = ArgumentCaptor.forClass(ImageType.class);
    verify(this.imageTypeDao, times(1)).createImageType(imageTypeArgumentCaptor.capture());
    final ImageType imageType = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("kafka_push_job", imageType.getName());
    Assert.assertEquals("azkaban", imageType.getCreatedBy());
    Assert.assertEquals("image", imageType.getDeployable().getName());
    Assert.assertNotNull(imageType.getOwnerships());
    Assert.assertEquals(2, imageType.getOwnerships().size());
    Assert.assertEquals(100, imageTypeId);
  }

  @Test
  public void testCreateImageTypeForConfigs() throws Exception {
    final String jsonPayload = JSONUtils.readJsonFileAsString("image_management/image_type_configs.json");
    final ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(this.imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    final int imageTypeId = this.imageTypeService.createImageType(imageMetadataRequest);
    final ArgumentCaptor<ImageType> imageTypeArgumentCaptor = ArgumentCaptor.forClass(ImageType.class);
    verify(this.imageTypeDao, times(1)).createImageType(imageTypeArgumentCaptor.capture());
    final ImageType imageType = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("configs", imageType.getName());
    Assert.assertEquals("azkaban", imageType.getCreatedBy());
    Assert.assertEquals("tar", imageType.getDeployable().getName());
    Assert.assertEquals(100, imageTypeId);
  }

  @Test(expected = ImageMgmtValidationException.class)
  public void testCreateImageTypeInvalidType() throws IOException {
    final String jsonPayload = JSONUtils.readJsonFileAsString("image_management/invalid_image_type.json");
    final ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(this.imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    this.imageTypeService.createImageType(imageMetadataRequest);

  }

  @Test(expected = ImageMgmtInvalidInputException.class)
  public void testCreateImageTypeInvalidDeployable() throws IOException {
    final String jsonPayload = JSONUtils
        .readJsonFileAsString("image_management/create_image_type_invalid_deployable.json");
    final ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(this.imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    this.imageTypeService.createImageType(imageMetadataRequest);

  }

  @Test(expected = ImageMgmtInvalidInputException.class)
  public void testCreateImageTypeInvalidRole() throws IOException {
    final String jsonPayload = JSONUtils
        .readJsonFileAsString("image_management/create_image_type_invalid_role.json");
    final ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(this.imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    this.imageTypeService.createImageType(imageMetadataRequest);

  }

}
