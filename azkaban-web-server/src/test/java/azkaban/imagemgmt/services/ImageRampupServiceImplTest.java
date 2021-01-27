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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.imagemgmt.converters.Converter;
import azkaban.imagemgmt.converters.ImageRampupPlanConverter;
import azkaban.imagemgmt.daos.ImageRampupDao;
import azkaban.imagemgmt.daos.ImageRampupDaoImpl;
import azkaban.imagemgmt.dto.ImageRampupPlanRequestDTO;
import azkaban.imagemgmt.dto.ImageRampupPlanResponseDTO;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.exception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageRampupPlan;
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
  private Converter<ImageRampupPlanRequestDTO, ImageRampupPlanResponseDTO, ImageRampupPlan> converter;

  @Before
  public void setup() {
    this.objectMapper = new ObjectMapper();
    this.imageRampupDao = mock(ImageRampupDaoImpl.class);
    this.converterUtils = new ConverterUtils(this.objectMapper);
    this.converter = new ImageRampupPlanConverter();
    this.imageRampupService = new ImageRampupServiceImpl(this.imageRampupDao, this.converter);
  }

  @Test
  public void testCreateImageRampup() throws Exception {
    final String jsonPayload = JSONUtils.readJsonFileAsString("image_management/create_image_rampup"
        + ".json");
    final ImageRampupPlanRequestDTO imageRampupPlanRequestDTO = this.converterUtils
        .convertToDTO(jsonPayload,
            ImageRampupPlanRequestDTO.class);
    imageRampupPlanRequestDTO.setCreatedBy("azkaban");
    imageRampupPlanRequestDTO.setModifiedBy("azkaban");
    when(this.imageRampupDao.createImageRampupPlan(any(ImageRampupPlan.class)))
        .thenReturn(100);
    final int imageRampupPlanId = this.imageRampupService
        .createImageRampupPlan(imageRampupPlanRequestDTO);
    final ArgumentCaptor<ImageRampupPlan> imageTypeArgumentCaptor = ArgumentCaptor
        .forClass(ImageRampupPlan.class);
    verify(this.imageRampupDao, times(1)).createImageRampupPlan(imageTypeArgumentCaptor.capture());
    final ImageRampupPlan capturedImageRampupPlanRequest = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("Rampup plan 1", capturedImageRampupPlanRequest.getPlanName());
    Assert.assertEquals("spark_job", capturedImageRampupPlanRequest.getImageTypeName());
    Assert.assertEquals(true, capturedImageRampupPlanRequest.getActivatePlan());
    Assert.assertEquals("azkaban", capturedImageRampupPlanRequest.getCreatedBy());
    Assert.assertEquals(2, capturedImageRampupPlanRequest.getImageRampups().size());
    Assert.assertEquals(100, imageRampupPlanId);
  }

  @Test(expected = ImageMgmtValidationException.class)
  public void testCreateImageRampupInvalidType() throws IOException {
    final String jsonPayload = JSONUtils
        .readJsonFileAsString("image_management/invalid_image_rampup_type_name.json");
    final ImageRampupPlanRequestDTO imageRampupPlanRequestDTO = this.converterUtils
        .convertToDTO(jsonPayload,
            ImageRampupPlanRequestDTO.class);
    imageRampupPlanRequestDTO.setCreatedBy("azkaban");
    imageRampupPlanRequestDTO.setModifiedBy("azkaban");
    when(this.imageRampupDao.createImageRampupPlan(any(ImageRampupPlan.class)))
        .thenReturn(100);
    this.imageRampupService.createImageRampupPlan(imageRampupPlanRequestDTO);
  }

  @Test(expected = ImageMgmtValidationException.class)
  public void testCreateImageRampupInvalidTotalPercentage() throws IOException {
    final String jsonPayload = JSONUtils
        .readJsonFileAsString("image_management/invalid_image_rampup_total_percentage.json");
    final ImageRampupPlanRequestDTO imageRampupPlanRequestDTO = this.converterUtils
        .convertToDTO(jsonPayload,
            ImageRampupPlanRequestDTO.class);
    imageRampupPlanRequestDTO.setCreatedBy("azkaban");
    imageRampupPlanRequestDTO.setModifiedBy("azkaban");
    when(this.imageRampupDao.createImageRampupPlan(any(ImageRampupPlan.class)))
        .thenReturn(100);
    this.imageRampupService.createImageRampupPlan(imageRampupPlanRequestDTO);
  }

  @Test(expected = ImageMgmtValidationException.class)
  public void testCreateImageRampupDuplicateVersion() throws IOException {
    final String jsonPayload = JSONUtils
        .readJsonFileAsString("image_management/invalid_image_rampup_duplicate_version.json");
    final ImageRampupPlanRequestDTO imageRampupPlanRequestDTO = this.converterUtils
        .convertToDTO(jsonPayload,
            ImageRampupPlanRequestDTO.class);
    imageRampupPlanRequestDTO.setCreatedBy("azkaban");
    imageRampupPlanRequestDTO.setModifiedBy("azkaban");
    when(this.imageRampupDao.createImageRampupPlan(any(ImageRampupPlan.class)))
        .thenReturn(100);
    this.imageRampupService.createImageRampupPlan(imageRampupPlanRequestDTO);

  }

  @Test
  public void testUpdateImageRampup() throws Exception {
    final String jsonPayload = JSONUtils.readJsonFileAsString("image_management/update_image_rampup"
        + ".json");
    final ImageRampupPlanRequestDTO imageRampupPlanRequestDTO = this.converterUtils
        .convertToDTO(jsonPayload,
            ImageRampupPlanRequestDTO.class);
    imageRampupPlanRequestDTO.setImageTypeName("spark_job");
    imageRampupPlanRequestDTO.setCreatedBy("azkaban");
    imageRampupPlanRequestDTO.setModifiedBy("azkaban");
    doNothing().doThrow(new ImageMgmtException("")).when(this.imageRampupDao)
        .updateImageRampupPlan(any(ImageRampupPlan.class));
    this.imageRampupService.updateImageRampupPlan(imageRampupPlanRequestDTO);
    final ArgumentCaptor<ImageRampupPlan> imageTypeArgumentCaptor = ArgumentCaptor
        .forClass(ImageRampupPlan.class);
    verify(this.imageRampupDao, times(1)).updateImageRampupPlan(imageTypeArgumentCaptor.capture());
    final ImageRampupPlan capturedImageRampupPlanRequest = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("Rampup plan 1", capturedImageRampupPlanRequest.getPlanName());
    Assert.assertEquals("spark_job", capturedImageRampupPlanRequest.getImageTypeName());
    Assert.assertEquals(true, capturedImageRampupPlanRequest.getActivatePlan());
    Assert.assertEquals("azkaban", capturedImageRampupPlanRequest.getModifiedBy());
    Assert.assertEquals(2, capturedImageRampupPlanRequest.getImageRampups().size());
  }

}
