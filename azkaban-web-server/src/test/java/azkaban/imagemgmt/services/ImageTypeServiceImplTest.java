package azkaban.imagemgmt.services;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.daos.ImageTypeDaoImpl;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.models.RequestContext;
import azkaban.imagemgmt.utils.JsonUtils;
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

  @Before
  public void setup() {
    this.objectMapper = new ObjectMapper();
    this.imageTypeDao = mock(ImageTypeDaoImpl.class);
    this.imageTypeService = new ImageTypeServiceImpl(imageTypeDao, objectMapper);
  }

  @Test
  public void testCreateImageType() throws Exception{
    String jsonPayload = JsonUtils.readJsonAsString("image_management/image_type.json");
    RequestContext requestContext = RequestContext.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    int imageTypeId = imageTypeService.createImageType(requestContext);
    ArgumentCaptor<ImageType> imageTypeArgumentCaptor = ArgumentCaptor.forClass(ImageType.class);
    verify(imageTypeDao, times(1)).createImageType(imageTypeArgumentCaptor.capture());
    ImageType imageType = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("kafka_push_job", imageType.getType());
    Assert.assertEquals("azkaban", imageType.getCreatedBy());
    Assert.assertEquals("image", imageType.getDeployable().getName());
    Assert.assertNotNull(imageType.getOwnerships());
    Assert.assertEquals(2, imageType.getOwnerships().size());
    Assert.assertEquals(100, imageTypeId);
  }

  @Test
  public void testCreateImageTypeForConfigs() throws Exception{
    String jsonPayload = JsonUtils.readJsonAsString("image_management/image_type_configs.json");
    RequestContext requestContext = RequestContext.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    int imageTypeId = imageTypeService.createImageType(requestContext);
    ArgumentCaptor<ImageType> imageTypeArgumentCaptor = ArgumentCaptor.forClass(ImageType.class);
    verify(imageTypeDao, times(1)).createImageType(imageTypeArgumentCaptor.capture());
    ImageType imageType = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("configs", imageType.getType());
    Assert.assertEquals("azkaban", imageType.getCreatedBy());
    Assert.assertEquals("tar", imageType.getDeployable().getName());
    Assert.assertEquals(100, imageTypeId);
  }

  @Test(expected = ImageMgmtValidationException.class)
  public void testCreateImageTypeInvalidType() throws IOException {
    String jsonPayload = JsonUtils.readJsonAsString("image_management/invalid_image_type.json");
    RequestContext requestContext = RequestContext.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageTypeDao.createImageType(any(ImageType.class))).thenReturn(100);
    imageTypeService.createImageType(requestContext);

  }

}
