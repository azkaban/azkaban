package azkaban.reportal.util.tableau;

import java.io.IOException;
import java.net.URL;
import org.apache.commons.io.IOUtils;

/**
 * URL Response is a class used by Tableau Job to interact
 * with the proxy server which interfaces with the Tableau
 * server.
 */
public class URLResponse {

  private final URL _url;
  private String _urlContents;

  public URLResponse(final String tableauUrl, final Path path, final String workbook) throws
      Exception {
    this._url = new URL(tableauUrl + "/" + path.getPath() + workbook);
    refreshContents();
  }

  public void refreshContents() throws IOException {
    this._urlContents = IOUtils.toString(this._url.openStream(), "UTF-8");
  }

  public String getContents() {
    return (this._urlContents);
  }

  private Boolean indicates(final String word) {
    if (this._urlContents == null) {
      return false;
    } else {
      return this._urlContents.contains(word);
    }
  }

  public Boolean indicatesSuccess() {
    return indicates("Success");
  }

  public Boolean indicatesError() {
    return indicates("Error");
  }

  public enum Path {
    REFRESH_EXTRACT("tableau_refresh_extract?workbook="), LAST_EXTRACT_STATUS(
        "tableau_last_extract_status?workbook=");

    private final String path;

    Path(final String pathContents) {
      this.path = pathContents;
    }

    public String getPath() {
      return (this.path);
    }
  }

}
