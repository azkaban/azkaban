/*
 * Copyright 2017 LinkedIn Corp.
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

package azkaban.db;

/**
 * Used for when we store text data. Plain uses UTF8 encoding.
 */
// TODO kunkun-tang: This class needs to move to azkaban-db module, as database module should be
// Deprecated soon.
public enum EncodingType {
  PLAIN(1), GZIP(2);

  private final int numVal;

  EncodingType(final int numVal) {
    this.numVal = numVal;
  }

  public static EncodingType fromInteger(final int x) {
    switch (x) {
      case 1:
        return PLAIN;
      case 2:
        return GZIP;
      default:
        return PLAIN;
    }
  }

  public int getNumVal() {
    return this.numVal;
  }
}
