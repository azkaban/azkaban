package azkaban.imagemgmt.services;

import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.models.RequestContext;
import java.io.IOException;

public interface ImageTypeService {
  public int createImageType(RequestContext requestContext) throws IOException,
      ImageMgmtException;
}
