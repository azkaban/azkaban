/*
 * Copyright 2021 LinkedIn Corp.
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
package azkaban.imagemgmt.dto;

import azkaban.imagemgmt.exception.ErrorCode;
import java.util.Optional;

/**
 * Response DTO for representing delete response
 */
public class DeleteResponse {

  private Optional<Object> data = Optional.empty();
  private Optional<ErrorCode> errorCode = Optional.empty();
  private String message;

  public boolean hasErrors() {
    return this.errorCode.isPresent() ? true : false;
  }

  public Optional<Object> getData() {
    return this.data;
  }

  public void setData(final Object data) {
    this.data = Optional.ofNullable(data);
  }

  public Optional<ErrorCode> getErrorCode() {
    return this.errorCode;
  }

  public void setErrorCode(final ErrorCode errorCode) {
    this.errorCode = Optional.ofNullable(errorCode);
  }

  public String getMessage() {
    return this.message;
  }

  public void setMessage(final String message) {
    this.message = message;
  }
}
