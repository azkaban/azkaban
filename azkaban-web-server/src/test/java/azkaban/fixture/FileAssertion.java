package azkaban.fixture;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.Assert;


/**
 * The type File assertion.
 */
public class FileAssertion {
  /**
   * Assert the given string equals the given file's content.
   *
   * @param expectedFilePath the expected file path
   * @param actual the actual string
   * @throws IOException the io exception
   */
  public static void assertStringEqualFileContent(String expectedFilePath, String actual)
      throws IOException {
    String expected = new String(Files.readAllBytes(Paths.get(expectedFilePath)), StandardCharsets.UTF_8);
    Assert.assertEquals(expected, actual);
  }

  /**
   * Return the html based content surrounded with the HTML tag.
   *
   * This is useful to compare a fragment of HTML content with a proper expected HTML file
   * so that the expected file can be viewed more easily with a browser.
   *
   * @param content the content
   * @return string
   */
  public static String surroundWithHtmlTag(String content) {
    return "<html>\n" + content + "</html>\n";
  }
}
