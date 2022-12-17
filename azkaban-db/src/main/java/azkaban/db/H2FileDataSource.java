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

import azkaban.utils.Props;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class H2FileDataSource extends AzkabanDataSource {

  @Inject
  public H2FileDataSource(final Props props) {
    super();
    final String filePath = props.getString("h2.path");
    final Path h2DbPath = Paths.get(filePath).toAbsolutePath();
    final String url = "jdbc:h2:file:" + h2DbPath + ";IGNORECASE=TRUE";
    setDriverClassName("org.h2.Driver");
    setJdbcUrl(url);
  }

  @Override
  public String getDBType() {
    return "h2";
  }

  @Override
  public boolean allowsOnDuplicateKey() {
    return false;
  }
}
