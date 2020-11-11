package azkaban.imagemgmt.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;

public class JsonUtils {

  public static String readJsonAsString(String filePath) {
    try {
      InputStream is = JsonUtils.class.getClassLoader().getResourceAsStream(filePath);
      return IOUtils.toString(is, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("Exception while reading input file : "+filePath);
    } catch (Exception ex) {
      throw new RuntimeException("Exception while reading input file : "+filePath);
    }
  }
}
