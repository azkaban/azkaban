package azkaban.imagemgmt.models;

/**
 * This class represents HP Flow managers' metadata
 */
public class HPFlowOwnership extends BaseModel {
  // HP flow management owner name
  private String name;

  public String getName() {
    return this.name;
  }

  public void setName(final String name) {
    this.name = name;
  }
}
