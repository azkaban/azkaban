package azkaban.imagemgmt.exeception;

public class ImageMgmtException extends RuntimeException {
  public ImageMgmtException(String errorMessage) {
    super(errorMessage);
  }

  public ImageMgmtException(String errorMessage, Throwable throwable) {
    super(errorMessage, throwable);
  }
}
