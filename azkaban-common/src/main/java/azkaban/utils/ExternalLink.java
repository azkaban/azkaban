package azkaban.utils;

/**
 * This is Object for external Link.
 * It stores information about external link, includes:
 * topic, label, linkUrl, isValid.
 * The object is used by web server to render external link.
 * It is used by AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPICS right now, and it can be reused
 * by AZKABAN_SERVER_EXTERNAL_LOGVIEWER_TOPIC later.
 */
public class ExternalLink {

  private final String topic;
  private final String label;
  private final String linkUrl;
  private final boolean linkUrlValid;

  public ExternalLink(final String topic, final String label,
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
        ", linkUrlValid='" + this.linkUrlValid + '\'' +
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
