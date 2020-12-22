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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * This class contains error code enum based on HttpStatus code and the error code can directly be
 * used as HttpStatus code.
 */
public enum ErrorCode {

  BAD_REQUEST(400),
  NOT_FOUND(404),
  CONFLICT(409),
  UNPROCESSABLE_ENTITY(422),
  INTERNAL_SERVER_ERROR(500),
  UNAUTHORIZED(401),
  FORBIDDEN(403);

  private static final Map<Integer, ErrorCode> CODE_LOOKUP = initialize();
  private final int code;

  /**
   * Initialize an ErrorCode based on the given code.
   *
   * @param code an int representing a valid error code
   */
  ErrorCode(final int code) {
    this.code = code;
  }

  /**
   * Return the HttpStatus associated with the given status code.
   *
   * @param code an int representing a valid http status code
   * @return and HttpStatus
   */
  public static ErrorCode fromCode(final int code) {
    final ErrorCode errorCodeEnum = CODE_LOOKUP.get(code);
    if (errorCodeEnum == null) {
      throw new IllegalArgumentException();
    }

    return errorCodeEnum;
  }

  private static Map<Integer, ErrorCode> initialize() {
    final Map<Integer, ErrorCode> result = new HashMap<>(ErrorCode.values().length);
    for (final ErrorCode errorCode : ErrorCode.values()) {
      result.put(errorCode.getCode(), errorCode);
    }
    return Collections.unmodifiableMap(result);
  }

  /**
   * @return the error code associated with this ErrorCode
   */
  public int getCode() {
    return this.code;
  }
}