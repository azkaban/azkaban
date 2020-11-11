package azkaban.imagemgmt.daos;

import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.RequestContext;
import java.util.List;

public interface ImageVersionDao {
  public int createImageVersion(ImageVersion imageVersion);
  public List<ImageVersion> getImageVersion(RequestContext requestContext) throws ImageMgmtException;
}
