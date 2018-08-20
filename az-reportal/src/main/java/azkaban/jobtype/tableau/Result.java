package azkaban.jobtype.tableau;

/**
 * Result enum stores resulting information from
 * the Tableau refresh
 */
enum Result {
  SUCCESS("SUCCESS"), FAIL("FAILURE"), TIMEOUT("TIMEOUT");

  private final String message;

  Result(final String resultMessage) {
    this.message = "The refresh finished with status: " + resultMessage + ".\n"
        + "See logs for more information.";
  }

  protected String getMessage() {
    return this.message;
  }

}
