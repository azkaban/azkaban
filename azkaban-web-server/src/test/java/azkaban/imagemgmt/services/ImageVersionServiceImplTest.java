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
import azkaban.imagemgmt.converters.ImageVersionConverter;
import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.daos.ImageVersionDaoImpl;
import azkaban.imagemgmt.dto.ImageVersionDTO;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.exception.ImageMgmtInvalidInputException;
import azkaban.imagemgmt.exception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.utils.JSONUtils;
import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ImageVersionServiceImplTest {

  private ImageVersionDao imageVersionDao;
  private ObjectMapper objectMapper;
  private ImageVersionService imageVersionService;
  private ConverterUtils converterUtils;
  private Converter<ImageVersionDTO, ImageVersionDTO, ImageVersion> converter;

  @Before
  public void setup() {
    this.objectMapper = new ObjectMapper();
    this.imageVersionDao = mock(ImageVersionDaoImpl.class);
    this.converterUtils = new ConverterUtils(this.objectMapper);
    this.converter = new ImageVersionConverter();
    this.imageVersionService = new ImageVersionServiceImpl(this.imageVersionDao, this.converter);
  }

  @Test
  public void testCreateImageVersion() throws Exception {
    final String jsonPayload = JSONUtils.readJsonFileAsString("image_management/image_version.json");
    final ImageVersionDTO imageVersionDTO = this.converterUtils.convertToDTO(jsonPayload,
        ImageVersionDTO.class);
    imageVersionDTO.setCreatedBy("azkaban");
    imageVersionDTO.setModifiedBy("azkaban");
    when(this.imageVersionDao.createImageVersion(any(ImageVersion.class))).thenReturn(100);
    final int imageVersionId = this.imageVersionService.createImageVersion(imageVersionDTO);
    final ArgumentCaptor<ImageVersion> imageTypeArgumentCaptor = ArgumentCaptor
        .forClass(ImageVersion.class);
    verify(this.imageVersionDao, times(1)).createImageVersion(imageTypeArgumentCaptor.capture());
    final ImageVersion capturedImageVersion = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("path_spark_job", capturedImageVersion.getPath());
    Assert.assertEquals("1.1.1", capturedImageVersion.getVersion());
    Assert.assertEquals("spark_job", capturedImageVersion.getName());
    Assert.assertEquals("azkaban", capturedImageVersion.getCreatedBy());
    Assert.assertEquals("new", capturedImageVersion.getState().getStateValue());
    Assert.assertEquals("1.2.0", capturedImageVersion.getReleaseTag());
    Assert.assertEquals(100, imageVersionId);
  }

  @Test(expected = ImageMgmtValidationException.class)
  public void testCreateImageVersionInvalidType() throws IOException {
    final String jsonPayload = JSONUtils
        .readJsonFileAsString("image_management/invalid_image_version.json");
    final ImageVersionDTO imageVersionDTO = this.converterUtils.convertToDTO(jsonPayload,
        ImageVersionDTO.class);
    imageVersionDTO.setCreatedBy("azkaban");
    imageVersionDTO.setModifiedBy("azkaban");
    when(this.imageVersionDao.createImageVersion(any(ImageVersion.class))).thenReturn(100);
    this.imageVersionService.createImageVersion(imageVersionDTO);
  }

  @Test(expected = ImageMgmtInvalidInputException.class)
  public void testCreateImageVersionInvalidState() throws IOException {
    final String jsonPayload = JSONUtils
        .readJsonFileAsString("image_management/create_image_version_invalid_state.json");
    final ImageVersionDTO imageVersionDTO = this.converterUtils.convertToDTO(jsonPayload,
        ImageVersionDTO.class);
    imageVersionDTO.setCreatedBy("azkaban");
    imageVersionDTO.setModifiedBy("azkaban");
    when(this.imageVersionDao.createImageVersion(any(ImageVersion.class))).thenReturn(100);
    this.imageVersionService.createImageVersion(imageVersionDTO);
  }

  @Test
  public void testUpdateImageVersion() throws Exception {
    final String jsonPayload = JSONUtils.readJsonFileAsString("image_management/update_image_version"
        + ".json");
    final ImageVersionDTO imageVersionDTO = this.converterUtils.convertToDTO(jsonPayload,
        ImageVersionDTO.class);
    imageVersionDTO.setId(11);
    imageVersionDTO.setCreatedBy("azkaban");
    imageVersionDTO.setModifiedBy("azkaban");
    doNothing().doThrow(new ImageMgmtException("")).when(this.imageVersionDao)
        .updateImageVersion(any(ImageVersion.class));
    this.imageVersionService.updateImageVersion(imageVersionDTO);
    final ArgumentCaptor<ImageVersion> imageVersionArgumentCaptor = ArgumentCaptor
        .forClass(ImageVersion.class);
    verify(this.imageVersionDao, times(1)).updateImageVersion(imageVersionArgumentCaptor.capture());
    final ImageVersion imageVersionRequest = imageVersionArgumentCaptor.getValue();
    Assert.assertEquals(11, imageVersionRequest.getId());
    Assert.assertEquals("Good active version", imageVersionRequest.getDescription());
    Assert.assertEquals("active", imageVersionRequest.getState().getStateValue());
    Assert.assertEquals("azkaban", imageVersionRequest.getModifiedBy());
  }
}
