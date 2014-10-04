package azkaban.project.validator;

/**
 * Status of the ValidationReport. It also represents the severity of each rule.
 * The order of severity for the status is PASS < WARN < ERROR.
 */
public enum ValidationStatus {
  PASS("PASS"),
  WARN("WARN"),
  ERROR("ERROR");

  private final String _status;

  private ValidationStatus(final String status) {
    _status = status;
  }

  @Override
  public String toString() {
    return _status;
  }
}
