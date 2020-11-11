package azkaban.imagemgmt.exeception;

public class ImageMgmtValidationException extends ImageMgmtException{
  public ImageMgmtValidationException(String errorMessage) {
    super(errorMessage);
  }
}
