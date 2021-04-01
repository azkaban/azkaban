package azkaban.project;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Dataset{
  String type;
  String name;
  String path;
  Map<String, String> annotation;

  public Dataset() {
  }

  public Dataset(String type, String name, String path, Map<String, String> annotation) {
    this.type = type;
    this.name = name;
    this.path = path;
    this.annotation = annotation;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public Map<String, String> getAnnotation() {
    return annotation;
  }

  public void setAnnotation(Map<String, String> annotation) {
    this.annotation = annotation;
  }

  @Override
  public String toString() {
    return "Dataset{" + "type='" + type + '\'' + ", name='" + name + '\'' + ", path='" + path + '\'' + ", annotation='"
        + annotation + '\'' + '}';
  }
}
