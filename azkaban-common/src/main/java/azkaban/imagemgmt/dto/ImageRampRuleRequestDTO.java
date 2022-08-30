package azkaban.imagemgmt.dto;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.codehaus.jackson.annotate.JsonProperty;


public class ImageRampRuleRequestDTO extends BaseDTO {
  // Represents the name of the Ramp rule
  @JsonProperty("ruleId")
  @NotBlank(message = "ruleId cannot be blank.", groups = ValidationOnCreate.class)
  @NotNull(message = "ruleId cannot be null.")
  private String ruleId;
  // Represents the name of the image type for the rule
  @JsonProperty("imageName")
  @NotBlank(message = "imageName cannot be blank.", groups = {ValidationOnCreate.class})
  @NotNull(message = "imageName cannot be null.")
  private String imageName;
  // Represents the name of the image version for the rule
  @JsonProperty("imageVersion")
  @NotBlank(message = "imageVersion cannot be blank.", groups = {ValidationOnCreate.class})
  @NotNull(message = "imageVersion cannot be null.")
  private String imageVersion;

  public void setRuleId(String ruleId) {
    this.ruleId = ruleId;
  }

  public void setImageName(String imageName) {
    this.imageName = imageName;
  }

  public void setImageVersion(String imageVersion) {
    this.imageVersion = imageVersion;
  }

  public String getRuleId() {
    return ruleId;
  }

  public String getImageName() {
    return imageName;
  }

  public String getImageVersion() {
    return imageVersion;
  }

}
