package azkaban.imagemgmt.models;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonValue;
import javax.validation.constraints.NotBlank;

public class ImageVersion extends BaseModel {
  private int id;
  @JsonProperty("imagePath")
  @NotBlank(message = "imagePath cannot be blank")
  private String path;
  @JsonProperty("imageVersion")
  @NotBlank(message = "imageVersion cannot be blank")
  private String version;
  @JsonProperty("imageType")
  @NotBlank(message = "imageType cannot be blank")
  private String type;
  private String description;
  @JsonProperty("versionState")
  private State state;
  private String releaseTag;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public State getState() {
    return state;
  }

  public void setState(State state) {
    this.state = state;
  }

  public String getReleaseTag() {
    return releaseTag;
  }

  public void setReleaseTag(String releaseTag) {
    this.releaseTag = releaseTag;
  }

  public enum State {
    NEW("new"),
    ACTIVE("active"),
    UNSTABLE("unstable"),
    DEPRECATED("deprecated");
    private final String stateValue;
    private State(String stateValue) {
      this.stateValue = stateValue;
    }

    private static final ImmutableMap<String, State> stateMap = Arrays.stream(State.values())
        .collect(ImmutableMap.toImmutableMap(state -> state.getStateValue(), state -> state));

    @JsonValue
    public String getStateValue() {
      return stateValue;
    }

    public static State fromStateValue(final String stateValue) {
      return stateMap.getOrDefault(stateValue, NEW);
    }
  }
}
