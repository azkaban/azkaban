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

import java.io.File;
import org.apache.commons.dbutils.QueryRunner;

public class AzDBTestUtility {

  public static DatabaseOperator initQuartzDB() throws Exception {
    final AzkabanDataSource dataSource = new EmbeddedH2BasicDataSource();

    final String sqlScriptsDir = new File("../azkaban-web-server/src/test/resources/")
        .getCanonicalPath();

    final DatabaseSetup setup = new DatabaseSetup(dataSource, sqlScriptsDir);
    setup.updateDatabase();
    return new DatabaseOperator(new QueryRunner(dataSource));
  }

  public static class EmbeddedH2BasicDataSource extends AzkabanDataSource {

    public EmbeddedH2BasicDataSource() {
      super();
      final String url = "jdbc:h2:mem:test;IGNORECASE=TRUE";
      setDriverClassName("org.h2.Driver");
      setUrl(url);
    }

    @Override
    public String getDBType() {
      return "h2-in-memory";
    }

    @Override
    public boolean allowsOnDuplicateKey() {
      return false;
    }
  }
}
