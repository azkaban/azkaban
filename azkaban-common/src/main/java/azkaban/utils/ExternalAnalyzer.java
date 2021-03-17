package azkaban.utils;

/**
 * This is Object for external analyzer.
 * It stores analyzer topic, label, linkUrl, isValid, used by web server to render external link
 */
public class ExternalAnalyzer {

  private final String topic;
  private final String label;
  private final String linkUrl;
  private final boolean linkUrlValid;

  public ExternalAnalyzer(final String topic, final String label,
      final String linkUrl, final boolean isLinkValid) {
    this.topic = topic;
    this.label = label;
    this.linkUrl = linkUrl;
    this.linkUrlValid = isLinkValid;
  }

  @Override
  public String toString() {
    return "ExternalAnalyzer{" +
        ", topic='" + this.topic + '\'' +
        ", label='" + this.label + '\'' +
        ", linkUrl='" + this.linkUrl + '\'' +
        '}';
  }

  public String getTopic() {
    return this.topic;
  }
  public String getLabel() {
    return this.label;
  }
  public String getLinkUrl() {
    return this.linkUrl;
  }
  public boolean isLinkUrlValid() {
    return this.linkUrlValid;
  }

}
