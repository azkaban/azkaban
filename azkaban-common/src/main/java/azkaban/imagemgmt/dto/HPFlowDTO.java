package azkaban.imagemgmt.dto;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.validation.constraints.NotBlank;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * This is Add HP Flow class for updating high priority flow metadata.
 */
public class HPFlowDTO extends BaseDTO {
  // CSV of flow ids.
  @JsonProperty("flowIds")
  @NotBlank(message = "flowList cannot be empty.")
  private String flowIds;

  public void setFlowIds(final String flowIds) {
    // Remove all whitespaces
    this.flowIds = flowIds.replaceAll("\\s", "");
  }

  public String getFlowIds() {
    return this.flowIds;
  }

  /**
   * Converts the CSV into a list of flow IDs.
   * @return list of flow IDs.
   */
  public List<String> getFlowIdList() {
    if (this.flowIds.isEmpty()) {
      return new ArrayList<>();
    }
    return Arrays.asList(flowIds.split(","));
  }

  @Override
  public String toString() {
    return "HPFlowDTO{" + "flowList='" + this.flowIds + '\'' + '}';
  }
}
