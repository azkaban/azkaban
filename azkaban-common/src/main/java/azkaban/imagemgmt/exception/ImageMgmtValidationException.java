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
package azkaban.imagemgmt.exception;

/**
 * The represents the exception class to be thrown on failure of API input validation
 */
public class ImageMgmtValidationException extends ImageMgmtException {

  public ImageMgmtValidationException(final String errorMessage) {
    super(errorMessage);
  }

  public ImageMgmtValidationException(final ErrorCode errorCode, final String errorMessage) {
    super(errorCode, errorMessage);
  }
}
