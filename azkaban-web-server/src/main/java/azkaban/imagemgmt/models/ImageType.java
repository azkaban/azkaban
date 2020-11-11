package azkaban.imagemgmt.models;

import java.util.List;
import javax.validation.constraints.NotBlank;
import org.codehaus.jackson.annotate.JsonProperty;

public class ImageType extends BaseModel {
  private int id;
  @JsonProperty("imageType")
  @NotBlank(message = "ImageType cannot be blank")
  private String type;
  private String description;
  private Deployable deployable;
  private List<ImageOwnership> ownerships;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Deployable getDeployable() {
    return deployable;
  }

  public void setDeployable(Deployable deployable) {
    this.deployable = deployable;
  }

  public List<ImageOwnership> getOwnerships() {
    return ownerships;
  }

  public void setOwnerships(List<ImageOwnership> ownerships) {
    this.ownerships = ownerships;
  }

  @Override
  public String toString() {
    return "ImageType{" +
        "id=" + id +
        ", type='" + type + '\'' +
        ", description='" + description + '\'' +
        ", deployable='" + deployable + '\'' +
        ", ownerships=" + ownerships +
        '}';
  }

  public enum Deployable {
    IMAGE("image"),
    TAR("tar"),
    JAR("jar");

    private Deployable(String name) {
      this.name = name;
    }

    private final String name;

    public String getName() {
      return name;
    }
  }
}
