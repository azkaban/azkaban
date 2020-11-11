package azkaban.imagemgmt.services;

import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.RequestContext;
import java.io.IOException;
import java.util.List;

public interface ImageVersionService {

  public int createImageVersion(RequestContext requestContext) throws IOException,
      ImageMgmtException;
  public List<ImageVersion> getImageVersion(RequestContext requestContext) throws ImageMgmtException;

}
