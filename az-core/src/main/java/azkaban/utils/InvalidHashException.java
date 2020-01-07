/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.utils;

/**
 * Indicates that a base64 encoded hash string (MD5 or SHA1) is invalid (wrong length, invalid characters, etc.)
 */
public class InvalidHashException extends Exception {
  public InvalidHashException() {
    super();
  }

  public InvalidHashException(final String s) {
    super(s);
  }

  public InvalidHashException(final String s, final Exception e) {
    super(s, e);
  }
}
