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

import java.io.InputStream;
import java.io.PrintStream;

/**
 * Utility to execute queries against a Hive database.
 */
public interface HiveQueryExecutor {
  /**
   * Execute the specified quer(y|ies).
   *
   * @param q Query to be executed. Queries may include \n and mutliple,
   *          ;-delimited statements. The entire string is passed to Hive.
   *
   * @throws HiveQueryExecutionException if Hive cannont execute a query.
   */
  public void executeQuery(String q) throws HiveQueryExecutionException;

  /**
   * Redirect the query execution's stdout
   *
   * @param out
   */
  public void setOut(PrintStream out);

  /**
   * Redirect the query execution's stdin
   *
   * @param in
   */
  public void setIn(InputStream in);

  /**
   * Redirect the query execution's stderr
   *
   * @param err
   */
  public void setErr(PrintStream err);
}
