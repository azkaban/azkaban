/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban.jobtype;

import azkaban.crypto.Decryptions;
import azkaban.reportal.util.ReportalUtil;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class ReportalPrestoRunner extends ReportalAbstractRunner {

  public static final String JDBC_DRIVER_KEY = "presto.driver";
  public static final String PRESTO_USER = "presto.driver.user";
  public static final String DRIVER_URL = "presto.driver.jdbc.url";
  private static final String PRESTO_DRIVER_PROP_PREFIX = "presto.driver.";
  private static final String IMPERSONATED_USER_KEY = "presto.execute.user";

  public ReportalPrestoRunner(final String jobName, final Properties props) {
    super(props);

    Preconditions.checkArgument(props.containsKey(JDBC_DRIVER_KEY), "missing " + JDBC_DRIVER_KEY);
    Preconditions.checkArgument(props.containsKey(PRESTO_USER), "missing " + PRESTO_USER);
    Preconditions.checkArgument(props.containsKey(DRIVER_URL), "missing " + DRIVER_URL);
  }

  private String decrypt(final String encrypted, final String keyPath) throws IOException {
    final FileSystem fs = FileSystem.get(URI.create("file:///"), new Configuration());
    return new Decryptions()
        .decrypt(encrypted, keyPath, fs);
  }

  private Properties getProperties() throws IOException {
    final Properties connProperties = new Properties();
    final Map<String, String> prestoProps = this.props.getMapByPrefix(PRESTO_DRIVER_PROP_PREFIX);

    for (final Entry<String, String> entry : prestoProps.entrySet()) {
      final String key = entry.getKey();
      final String value = entry.getValue();
      if (key.contains("encrypted.")) {
        // if props contains "encrypted." then decrypted it with key file and put the decrypted
        // value into jdbc connection props
        //"encrypted.password" => "password"
        final String jdbcProp = key.replaceFirst("encrypted.", "");
        connProperties.put(jdbcProp, decrypt(value, prestoProps.get("jdbc.crypto.key.path")));
      } else if (!key.equals("jdbc.url") && !key.equals("jdbc.crypto.key.path")) {
        connProperties.put(key, value);
      }
    }

    return connProperties;
  }

  private Connection getConnection(final String jdbcUrl, final String userToProxy) {
    try {
      Class.forName(this.props.get(JDBC_DRIVER_KEY));
      final Properties connProperties = getProperties();
      connProperties.put(IMPERSONATED_USER_KEY, userToProxy);
      final Connection conn = DriverManager.getConnection(jdbcUrl, connProperties);
      return conn;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void runReportal() throws Exception {
    final Connection conn = getConnection(this.props.get(PRESTO_DRIVER_PROP_PREFIX + "jdbc.url"),
        this.proxyUser);
    final Statement statement = conn.createStatement();
    try {
      statement.execute(this.jobQuery);
      ReportalUtil.outputQueryResult(statement.getResultSet(), this.outputStream);
    } finally {
      statement.close();
      conn.close();
    }
  }
}
