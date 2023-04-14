package azkaban.utils;

import azkaban.Constants;
import java.util.Objects;

/**
 * Represents an external link.
 * Links are rendered as buttons in the Flow and/or Job execution pages. Its configuration is
 * optional, is done in {@value Constants#AZKABAN_PROPERTIES_FILE}.
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExternalLink link = (ExternalLink) o;
    return Objects.equals(this.topic, link.topic) &&
        Objects.equals(this.label, link.label) &&
        Objects.equals(this.linkUrl, link.linkUrl) &&
        this.linkUrlValid == link.linkUrlValid;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.topic, this.label, this.linkUrl, this.linkUrlValid);
  }
}
