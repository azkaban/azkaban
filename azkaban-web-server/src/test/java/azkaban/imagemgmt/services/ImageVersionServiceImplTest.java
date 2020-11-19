package azkaban.imagemgmt.services;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.daos.ImageVersionDaoImpl;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.RequestContext;
import azkaban.imagemgmt.utils.JsonUtils;
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

  @Before
  public void setup() {
    this.objectMapper = new ObjectMapper();
    this.imageVersionDao = mock(ImageVersionDaoImpl.class);
    this.imageVersionService = new ImageVersionServiceImpl(imageVersionDao, objectMapper);
  }

  @Test
  public void testCreateImageVersion() throws Exception{
    String jsonPayload = JsonUtils.readJsonAsString("image_management/image_version.json");
    RequestContext requestContext = RequestContext.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageVersionDao.createImageVersion(any(ImageVersion.class))).thenReturn(100);
    int imageVersionId = imageVersionService.createImageVersion(requestContext);
    ArgumentCaptor<ImageVersion> imageTypeArgumentCaptor = ArgumentCaptor.forClass(ImageVersion.class);
    verify(imageVersionDao, times(1)).createImageVersion(imageTypeArgumentCaptor.capture());
    ImageVersion imageVersion = imageTypeArgumentCaptor.getValue();
    Assert.assertEquals("path_spark_job", imageVersion.getPath());
    Assert.assertEquals("1.1.1", imageVersion.getVersion());
    Assert.assertEquals("spark_job", imageVersion.getType());
    Assert.assertEquals("azkaban", imageVersion.getCreatedBy());
    Assert.assertEquals("new", imageVersion.getState().getStateValue());
    Assert.assertEquals("1.2.0", imageVersion.getReleaseTag());
    Assert.assertEquals(100, imageVersionId);
  }

  @Test(expected = ImageMgmtValidationException.class)
  public void testCreateImageTypeInvalidType() throws IOException {
    String jsonPayload = JsonUtils.readJsonAsString("image_management/invalid_image_version.json");
    RequestContext requestContext = RequestContext.newBuilder()
        .jsonPayload(jsonPayload)
        .user("azkaban")
        .build();
    when(imageVersionDao.createImageVersion(any(ImageVersion.class))).thenReturn(100);
    imageVersionService.createImageVersion(requestContext);

  }

}
