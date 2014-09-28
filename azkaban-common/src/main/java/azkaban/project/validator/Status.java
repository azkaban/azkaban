package azkaban.project.validator;

/**
 * Status of the ValidationReport. It also represents the severity of each rule.
 */
public enum Status {
  PASS("PASS"),
  WARN("WARN"),
  ERROR("ERROR");

  private final String _status;

  private Status(final String status) {
    _status = status;
  }

  @Override
  public String toString() {
    return _status;
  }
}
