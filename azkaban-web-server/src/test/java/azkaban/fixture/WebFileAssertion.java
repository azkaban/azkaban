package azkaban.fixture;

import java.io.IOException;


/**
 * Utility to assert based on expected files in the webserver project.
 */
public class WebFileAssertion {
  private static final String EXPECTED_FILE_DIR = "src/test/expected/";

  /**
   * Assert the string equals file content in this project's expected file directory.
   *
   * @param expectedFileName the expected file name
   * @param actual the actual
   * @throws IOException the io exception
   */
  public static void assertStringEqualFileContent(String expectedFileName, String actual)
      throws IOException {
    String expectedFilePath = EXPECTED_FILE_DIR + expectedFileName;
    FileAssertion.assertStringEqualFileContent(expectedFilePath, actual);
  }
}
