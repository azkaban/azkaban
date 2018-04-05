/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.jobtype.hiveutils;

/**
 * Thrown when a query sent for execution ends unsuccessfully.
 */
public class HiveQueryExecutionException extends Exception {
  private static final long serialVersionUID = 1L;

  /**
   * Query that caused the failure.
   */
  private final String query;

  /**
   * Error code defined by Hive
   */
  private final int returnCode;

  public HiveQueryExecutionException(int returnCode, String query) {
    this.returnCode = returnCode;
    this.query = query;
  }

  public String getLine() {
    return query;
  }

  public int getReturnCode() {
    return returnCode;
  }

  @Override
  public String toString() {
    return "HiveQueryExecutionException{" + "query='" + query + '\''
        + ", returnCode=" + returnCode + '}';
  }
}
