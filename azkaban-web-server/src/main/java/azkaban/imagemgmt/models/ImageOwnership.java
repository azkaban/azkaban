package azkaban.imagemgmt.models;

public class ImageOwnership extends BaseModel {
  private String owner;
  private String role;

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public String getRole() {
    return role;
  }

  public void setRole(String role) {
    this.role = role;
  }

  @Override
  public String toString() {
    return "ImageOwnership{" +
        "owner='" + owner + '\'' +
        ", role='" + role + '\'' +
        '}';
  }
}
