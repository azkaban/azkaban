package azkaban.imagemgmt.daos;

import azkaban.imagemgmt.models.ImageType;
import java.util.Optional;

public interface ImageTypeDao {
  public int createImageType(ImageType imageType);
  public Optional<ImageType> getImageTypeByType(String type);
}
