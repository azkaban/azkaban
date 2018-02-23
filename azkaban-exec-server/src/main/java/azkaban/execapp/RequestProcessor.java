package azkaban.execapp;

import java.io.IOException;
import java.util.HashMap;
import javax.servlet.ServletException;

@FunctionalInterface
public interface RequestProcessor {

  void process(HashMap<String, Object> respMap, String action) throws ServletException, IOException;

}
