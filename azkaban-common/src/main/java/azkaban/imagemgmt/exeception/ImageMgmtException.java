/*
 * Copyright 2020 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.imagemgmt.exeception;

/**
 * Top level exception class for image management
 */
public class ImageMgmtException extends RuntimeException {

  // Initialize default to INTERNAL_SERVER_ERROR
  private ErrorCode errorCode = ErrorCode.INTERNAL_SERVER_ERROR;

  public ImageMgmtException(final String errorMessage) {
    super(errorMessage);
  }

  public ImageMgmtException(final ErrorCode errorCode, final String errorMessage) {
    super(errorMessage);
    this.errorCode = errorCode;
  }

  public ImageMgmtException(final String errorMessage, final Throwable throwable) {
    super(errorMessage, throwable);
  }

  public ImageMgmtException(final ErrorCode errorCode, final String errorMessage,
      final Throwable throwable) {
    super(errorMessage, throwable);
    this.errorCode = errorCode;
  }

  public ErrorCode getErrorCode() {
    return this.errorCode;
  }
}
