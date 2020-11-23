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

import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.daos.ImageVersionDaoImpl;
import azkaban.imagemgmt.exeception.ImageMgmtInvalidInputException;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.dto.ImageMetadataRequest;
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

  @Before
  public void setup() {
    this.objectMapper = new ObjectMapper();
    this.imageVersionDao = mock(ImageVersionDaoImpl.class);
    this.converterUtils = new ConverterUtils(objectMapper);
    this.imageVersionService = new ImageVersionServiceImpl(imageVersionDao, converterUtils);
  }

  @Test
  public void testCreateImageVersion() throws Exception{
    String jsonPayload = JSONUtils.readJsonFileAsString("image_management/image_version.json");
    ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    ImageVersion imageVersion1 = objectMapper.readValue(imageMetadataRequest.getJsonPayload(),
        ImageVersion.class);
    System.out.println(imageVersion1.getState().getStateValue());
    String json = objectMapper.writeValueAsString(imageVersion1);
    System.out.println(json);
    when(imageVersionDao.createImageVersion(any(ImageVersion.class))).thenReturn(100);
    int imageVersionId = imageVersionService.createImageVersion(imageMetadataRequest);
    ArgumentCaptor<ImageVersion> imageTypeArgumentCaptor = ArgumentCaptor.forClass(ImageVersion.class);
    verify(imageVersionDao, times(1)).createImageVersion(imageTypeArgumentCaptor.capture());
    ImageVersion imageVersion = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("path_spark_job", imageVersion.getPath());
    Assert.assertEquals("1.1.1", imageVersion.getVersion());
    Assert.assertEquals("spark_job", imageVersion.getName());
    Assert.assertEquals("azkaban", imageVersion.getCreatedBy());
    Assert.assertEquals("new", imageVersion.getState().getStateValue());
    Assert.assertEquals("1.2.0", imageVersion.getReleaseTag());
    Assert.assertEquals(100, imageVersionId);
  }

  @Test(expected = ImageMgmtValidationException.class)
  public void testCreateImageVersionInvalidType() throws IOException {
    String jsonPayload = JSONUtils.readJsonFileAsString("image_management/invalid_image_version.json");
    ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageVersionDao.createImageVersion(any(ImageVersion.class))).thenReturn(100);
    imageVersionService.createImageVersion(imageMetadataRequest);
  }

  @Test(expected = ImageMgmtInvalidInputException.class)
  public void testCreateImageVersionInvalidState() throws IOException {
    String jsonPayload = JSONUtils.readJsonFileAsString("image_management/create_image_version_invalid_state.json");
    ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageVersionDao.createImageVersion(any(ImageVersion.class))).thenReturn(100);
    imageVersionService.createImageVersion(imageMetadataRequest);

  }

}
