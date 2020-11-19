package azkaban.imagemgmt.services;

import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.ImageVersion.State;
import azkaban.imagemgmt.models.RequestContext;
import azkaban.imagemgmt.utils.ValidatorUtils;
import java.io.IOException;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.codehaus.jackson.map.ObjectMapper;

@Singleton
public class ImageVersionServiceImpl implements ImageVersionService {

  private final ImageVersionDao imageVersionsDao;
  private final ObjectMapper objectMapper;

  @Inject
  public ImageVersionServiceImpl(final ImageVersionDao imageVersionsDao, final ObjectMapper objectMapper) {
    this.imageVersionsDao = imageVersionsDao;
    this.objectMapper = objectMapper;
  }

  @Override
  public int createImageVersion(RequestContext requestContext)
      throws IOException, ImageMgmtException {
    ImageVersion imageVersion = objectMapper.readValue(requestContext.getJsonPayload(), ImageVersion.class);
    imageVersion.setCreatedBy(requestContext.getUser());
    if(imageVersion.getState() == null) {
      imageVersion.setState(State.NEW);
    }
    if(!ValidatorUtils.validateObject(imageVersion)) {
      throw new ImageMgmtValidationException("Provide valid input for creating image version "
          + "metadata");
    }
    return imageVersionsDao.createImageVersion(imageVersion);
  }

  @Override
  public List<ImageVersion> getImageVersion(RequestContext requestContext) throws ImageMgmtException {
    return imageVersionsDao.getImageVersion(requestContext);
  }
}
