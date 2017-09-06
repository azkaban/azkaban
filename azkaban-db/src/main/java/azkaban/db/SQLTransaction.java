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
 *
 */
package azkaban.db;

import java.sql.SQLException;


/**
 * This interface defines how a sequence of sql statements are organized and packed together. All
 * transaction implementations must follow this interface, and will be called in {@link
 * DatabaseOperator#transaction(SQLTransaction)}
 *
 * @param <T> The transaction return type
 */
@FunctionalInterface
public interface SQLTransaction<T> {

  public T execute(DatabaseTransOperator transOperator) throws SQLException;
}
