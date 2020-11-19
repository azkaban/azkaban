package azkaban.imagemgmt.services;

import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.models.RequestContext;
import azkaban.imagemgmt.utils.ValidatorUtils;
import java.io.IOException;
import javax.inject.Inject;
import org.codehaus.jackson.map.ObjectMapper;

public class ImageTypeServiceImpl implements ImageTypeService {

  private final ImageTypeDao imageTypeDao;
  private final ObjectMapper objectMapper;

  @Inject
  public ImageTypeServiceImpl(final ImageTypeDao imageTypeDao, final ObjectMapper objectMapper) {
    this.imageTypeDao = imageTypeDao;
    this.objectMapper = objectMapper;
  }

  @Override
  public int createImageType(RequestContext requestContext) throws IOException,
      ImageMgmtException {
    ImageType imageType = objectMapper.readValue(requestContext.getJsonPayload(), ImageType.class);
    imageType.setCreatedBy(requestContext.getUser());
    if(!ValidatorUtils.validateObject(imageType)) {
      throw new ImageMgmtValidationException("Provide valid input for creating image type "
          + "metadata");
    }
    return imageTypeDao.createImageType(imageType);
  }
}
