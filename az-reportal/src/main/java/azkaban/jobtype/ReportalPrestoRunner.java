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
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportalPrestoRunner extends ReportalAbstractRunner {

  private static final Logger logger = LoggerFactory.getLogger(ReportalPrestoRunner.class);
  private static final String IMPERSONATED_USER_KEY = "presto.execute.user";


  public ReportalPrestoRunner(final String jobName, final Properties props) {
    super(props);
  }

  static String filesToURIString(final File[] files) throws IOException {
    final StringBuffer sb = new StringBuffer();
    for (int i = 0; i < files.length; i++) {
      sb.append("file:///").append(files[i].getCanonicalPath());
      if (i != files.length - 1) {
        sb.append(",");
      }
    }

    return sb.toString();
  }


  private String decrypt(final String encrypted, final String keyPath) throws IOException {
    final FileSystem fs = FileSystem.get(URI.create("file:///"), new Configuration());
    return new Decryptions()
        .decrypt(encrypted, keyPath, fs);
  }

  private Properties getProperties() throws IOException {
    final Properties connProperties = new Properties();
    connProperties.put("jdbc.crypto.key.path", this.props.get("jdbc.crypto.key.path"));

    connProperties.put("user", this.props.get("user"));

    connProperties.put("password", decrypt(this.props.get("encrypted.password"),
        this.props.get("jdbc.crypto.key.path")));

    connProperties.put("SSLTrustStorePassword",
        decrypt(this.props.get("encrypted.SSLTrustStorePassword"),
            this.props.get("jdbc.crypto.key.path")));
    connProperties
        .put("SSLTrustStorePath", this.props.get("SSLTrustStorePath"));

    return connProperties;
  }

  public Connection getConnection(final String jdbcUrl, final String userToProxy) {
    try {
      Class.forName("com.linkedin.daptor.jdbc.DaptorDriver");
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

    final Connection conn = getConnection(this.props.get("jdbc.url"), this.proxyUser);
    final PreparedStatement statement = conn.prepareStatement(this.jobQuery);
    try {
      statement.execute();
      ReportalUtil.outputQueryResult(statement.getResultSet(), this.outputStream);
    } finally {
      statement.close();
      conn.close();
    }
  }

}
