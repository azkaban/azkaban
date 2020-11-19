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
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.daos.ImageTypeDaoImpl;
import azkaban.imagemgmt.exeception.ImageMgmtInvalidInputException;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.dto.RequestContext;
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
    this.converterUtils = new ConverterUtils(objectMapper);
    this.imageTypeService = new ImageTypeServiceImpl(imageTypeDao, converterUtils);
  }

  @Test
  public void testCreateImageType() throws Exception{
    String jsonPayload = JSONUtils.readJsonAsString("image_management/image_type.json");
    RequestContext requestContext = RequestContext.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    int imageTypeId = imageTypeService.createImageType(requestContext);
    ArgumentCaptor<ImageType> imageTypeArgumentCaptor = ArgumentCaptor.forClass(ImageType.class);
    verify(imageTypeDao, times(1)).createImageType(imageTypeArgumentCaptor.capture());
    ImageType imageType = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("kafka_push_job", imageType.getName());
    Assert.assertEquals("azkaban", imageType.getCreatedBy());
    Assert.assertEquals("image", imageType.getDeployable().getName());
    Assert.assertNotNull(imageType.getOwnerships());
    Assert.assertEquals(2, imageType.getOwnerships().size());
    Assert.assertEquals(100, imageTypeId);
  }

  @Test
  public void testCreateImageTypeForConfigs() throws Exception{
    String jsonPayload = JSONUtils.readJsonAsString("image_management/image_type_configs.json");
    RequestContext requestContext = RequestContext.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    int imageTypeId = imageTypeService.createImageType(requestContext);
    ArgumentCaptor<ImageType> imageTypeArgumentCaptor = ArgumentCaptor.forClass(ImageType.class);
    verify(imageTypeDao, times(1)).createImageType(imageTypeArgumentCaptor.capture());
    ImageType imageType = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("configs", imageType.getName());
    Assert.assertEquals("azkaban", imageType.getCreatedBy());
    Assert.assertEquals("tar", imageType.getDeployable().getName());
    Assert.assertEquals(100, imageTypeId);
  }

  @Test(expected = ImageMgmtValidationException.class)
  public void testCreateImageTypeInvalidType() throws IOException {
    String jsonPayload = JSONUtils.readJsonAsString("image_management/invalid_image_type.json");
    RequestContext requestContext = RequestContext.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    imageTypeService.createImageType(requestContext);

  }

  @Test(expected = ImageMgmtInvalidInputException.class)
  public void testCreateImageTypeInvalidDeployable() throws IOException {
    String jsonPayload = JSONUtils.readJsonAsString("image_management/create_image_type_invalid_deployable.json");
    RequestContext requestContext = RequestContext.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    imageTypeService.createImageType(requestContext);

  }

  @Test(expected = ImageMgmtInvalidInputException.class)
  public void testCreateImageTypeInvalidRole() throws IOException {
    String jsonPayload = JSONUtils.readJsonAsString("image_management/create_image_type_invalid_role.json");
    RequestContext requestContext = RequestContext.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    imageTypeService.createImageType(requestContext);

  }

}
