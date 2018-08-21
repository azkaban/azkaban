package azkaban.reportal.util.tableau;

/**
 * Result enum stores resulting information from
 * the Tableau refresh
 */
public enum Result {
  SUCCESS("SUCCESS"), FAIL("FAILURE"), TIMEOUT("TIMEOUT");

  private final String message;

  Result(final String resultMessage) {
    this.message = "The refresh finished with status: " + resultMessage + ".\n"
        + "See logs for more information.";
  }

  public String getMessage() {
    return this.message;
  }

}
