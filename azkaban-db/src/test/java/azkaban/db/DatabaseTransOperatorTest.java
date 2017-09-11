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

import org.apache.commons.dbutils.QueryRunner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

// todo kunkun-tang: complete this test.
public class DatabaseTransOperatorTest {

  @Before
  public void setUp() throws Exception {
    final AzkabanDataSource datasource = new AzDBTestUtility.EmbeddedH2BasicDataSource();
    final DatabaseTransOperator operator = new DatabaseTransOperator(new QueryRunner(),
        datasource.getConnection());
  }

  @Ignore
  @Test
  public void testQuery() throws Exception {
  }

  @Ignore
  @Test
  public void testUpdate() throws Exception {
  }
}
