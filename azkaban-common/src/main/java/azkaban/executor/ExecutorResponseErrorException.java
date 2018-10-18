package azkaban.executor;

import azkaban.executor.ConnectorParams.ResponseErrorType;

public class ExecutorResponseErrorException extends ExecutorManagerException {

  private final ResponseErrorType errorType;

  public ExecutorResponseErrorException(String msg, ResponseErrorType errorType) {
    super(msg);
    this.errorType = errorType;
  }

  public ExecutorResponseErrorException(String msg, Throwable t, ResponseErrorType errorType) {
    super(msg, t);
    this.errorType = errorType;
  }

  public ResponseErrorType getErrorType() {
    return errorType;
  }

}
