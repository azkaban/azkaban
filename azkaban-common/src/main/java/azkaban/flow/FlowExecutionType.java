package azkaban.flow;

public enum FlowExecutionType {
  SCHEDULED("scheduled"), ADHOC("adhoc");

  private final String value;

  FlowExecutionType(String s) {
    this.value = s;
  }

  public static FlowExecutionType fromString(String text) {
    for (FlowExecutionType type : FlowExecutionType.values()) {
      if (type.value.equalsIgnoreCase(text)) {
        return type;
      }
    }
    return null;
  }

  public static String getString(FlowExecutionType type) {
    if(type == null) {
      return null;
    }
    return type.value;
  }
}
