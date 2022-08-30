package azkaban.imagemgmt.models;

import java.util.List;
import java.util.Set;


public class ImageRampRule extends BaseModel {
  // ramp rule name
  String ruleId;
  // image name of ramp rule for
  String imageName;
  // image version of ramp rule for
  String imageVersion;
  // rule owners
  Set<String> owners;
  // Ramp Rule for HP flows or not
  boolean isHPRule;

  public ImageRampRule(String ruleId,
                String imageName,
                String imageVersion,
                Set<String> owners,
                boolean isHP) {
    this.ruleId = ruleId;
    this.imageName = imageName;
    this.imageVersion = imageVersion;
    this.owners = owners;
    this.isHPRule = isHP;
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

  public String getOwners() {
    return String.join(",", owners);
  }

  public boolean isHPRule() {
    return isHPRule;
  }
}
