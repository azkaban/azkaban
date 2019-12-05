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
 * Typically used when validation of a file checksum after downloading fails due to a checksum mismatch.
 */
public class HashNotMatchException extends Exception {
  public HashNotMatchException() {
    super();
  }

  public HashNotMatchException(final String s) {
    super(s);
  }

  public HashNotMatchException(final String s, final Exception e) {
    super(s, e);
  }
}
