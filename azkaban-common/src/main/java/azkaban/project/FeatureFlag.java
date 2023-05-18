package azkaban.project;

/**
 * A Feature Flag class contains enum of all feature flags.
 * */
public enum FeatureFlag {

  ENABLE_PROJECT_ADHOC_UPLOAD("enable.project.adhoc.upload");

  private final String name;

  FeatureFlag(String name) {
    this.name = name;
  }

  public static FeatureFlag fromString(String key) {
    for (FeatureFlag featureFlag : FeatureFlag.values()) {
      if (featureFlag.getName().equals(key)) {
        return featureFlag;
      }
    }
    throw new IllegalArgumentException("FeatureFlag not found for key: " + key);
  }

  @Override
  public String toString() {
    return getName();
  }

  public String getName() {
    return this.name;
  }

}

