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

import static azkaban.Constants.ImageMgmtConstants.IMAGE_TYPE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.imagemgmt.daos.ImageRampupDao;
import azkaban.imagemgmt.daos.ImageRampupDaoImpl;
import azkaban.imagemgmt.dto.ImageMetadataRequest;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageRampupPlanRequest;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.utils.JSONUtils;
import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ImageRampupServiceImplTest {

  private ImageRampupDao imageRampupDao;
  private ObjectMapper objectMapper;
  private ImageRampupService imageRampupService;
  private ConverterUtils converterUtils;

  @Before
  public void setup() {
    objectMapper = new ObjectMapper();
    imageRampupDao = mock(ImageRampupDaoImpl.class);
    converterUtils = new ConverterUtils(objectMapper);
    imageRampupService = new ImageRampupServiceImpl(imageRampupDao, converterUtils);
  }

  @Test
  public void testCreateImageRampup() throws Exception {
    String jsonPayload = JSONUtils.readJsonFileAsString("image_management/create_image_rampup"
        + ".json");
    ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageRampupDao.createImageRampupPlan(any(ImageRampupPlanRequest.class))).thenReturn(100);
    int imageRampupPlanId = imageRampupService.createImageRampupPlan(imageMetadataRequest);
    ArgumentCaptor<ImageRampupPlanRequest> imageTypeArgumentCaptor = ArgumentCaptor
        .forClass(ImageRampupPlanRequest.class);
    verify(imageRampupDao, times(1)).createImageRampupPlan(imageTypeArgumentCaptor.capture());
    ImageRampupPlanRequest imageRampupPlanRequest = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("Rampup plan 1", imageRampupPlanRequest.getPlanName());
    Assert.assertEquals("spark_job", imageRampupPlanRequest.getImageTypeName());
    Assert.assertEquals(true, imageRampupPlanRequest.isActivatePlan());
    Assert.assertEquals("azkaban", imageRampupPlanRequest.getCreatedBy());
    Assert.assertEquals(3, imageRampupPlanRequest.getImageRampups().size());
    Assert.assertEquals(100, imageRampupPlanId);
  }

  @Test(expected = ImageMgmtValidationException.class)
  public void testCreateImageRampupInvalidType() throws IOException {
    String jsonPayload = JSONUtils
        .readJsonFileAsString("image_management/invalid_image_rampup_type_name.json");
    ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageRampupDao.createImageRampupPlan(any(ImageRampupPlanRequest.class))).thenReturn(100);
    imageRampupService.createImageRampupPlan(imageMetadataRequest);
  }

  @Test(expected = ImageMgmtValidationException.class)
  public void testCreateImageRampupInvalidTotalPercentage() throws IOException {
    String jsonPayload = JSONUtils
        .readJsonFileAsString("image_management/invalid_image_rampup_total_percentage.json");
    ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageRampupDao.createImageRampupPlan(any(ImageRampupPlanRequest.class))).thenReturn(100);
    imageRampupService.createImageRampupPlan(imageMetadataRequest);
  }

  @Test(expected = ImageMgmtValidationException.class)
  public void testCreateImageRampupDuplicateVersion() throws IOException {
    String jsonPayload = JSONUtils
        .readJsonFileAsString("image_management/invalid_image_rampup_duplicate_version.json");
    ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageRampupDao.createImageRampupPlan(any(ImageRampupPlanRequest.class))).thenReturn(100);
    imageRampupService.createImageRampupPlan(imageMetadataRequest);

  }

  @Test
  public void testUpdateImageRampup() throws Exception {
    String jsonPayload = JSONUtils.readJsonFileAsString("image_management/update_image_rampup"
        + ".json");
    ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .jsonPayload(jsonPayload)
        .addParam(IMAGE_TYPE, "spark_job")
        .user("azkaban")
        .build();
    doNothing().doThrow(new ImageMgmtException("")).when(imageRampupDao)
        .updateImageRampupPlan(any(ImageRampupPlanRequest.class));
    imageRampupService.updateImageRampupPlan(imageMetadataRequest);
    ArgumentCaptor<ImageRampupPlanRequest> imageTypeArgumentCaptor = ArgumentCaptor
        .forClass(ImageRampupPlanRequest.class);
    verify(imageRampupDao, times(1)).updateImageRampupPlan(imageTypeArgumentCaptor.capture());
    ImageRampupPlanRequest imageRampupPlanRequest = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("Rampup plan 1", imageRampupPlanRequest.getPlanName());
    Assert.assertEquals("spark_job", imageRampupPlanRequest.getImageTypeName());
    Assert.assertEquals(true, imageRampupPlanRequest.isActivatePlan());
    Assert.assertEquals("azkaban", imageRampupPlanRequest.getModifiedBy());
    Assert.assertEquals(2, imageRampupPlanRequest.getImageRampups().size());
  }

}
