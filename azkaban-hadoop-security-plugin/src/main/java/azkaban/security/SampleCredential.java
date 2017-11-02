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

package azkaban.security;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.log4j.Logger;
/**
 * This class serves as an example how we implement Credential. The actual class should be checked
 * in a separate jar (gradle product).
 */
public class SampleCredential implements Credential {

  private final static Logger logger = Logger.getLogger(SampleCredential.class);

  // Sample secret key.
  private static final Text SECRET_KEY_NAME = new Text("li.datavault.identity");
  private static final Text EI_TRUSTSTORE = new Text("li.datavault.truststore");
  private final Credentials hadoopCrednetial;

  public SampleCredential(final Credentials hadoopCrednetial) {
    this.hadoopCrednetial = hadoopCrednetial;
  }

  @Override
  public void register(final String user) {
    this.hadoopCrednetial.addSecretKey(SECRET_KEY_NAME, getSecretKey(user));
    this.hadoopCrednetial.addSecretKey(EI_TRUSTSTORE, getTrustStore());
  }

  private byte[] getSecretKey(final String user) {
    // would call out to service
    final byte[] buffer = new byte[1024 * 1024];
    try {
      final FileInputStream input = new FileInputStream("/export/home/azkaban/identity.p12");
      final BufferedInputStream bufferedStream = new BufferedInputStream(input);
      final int size = bufferedStream.read(buffer);
      logger.info("Read bytes for " + ", size:" + size);
      return Arrays.copyOfRange(buffer, 0, size);

    } catch (final IOException e) {
      logger.error("one IO Exception.", e);
    }
    return null;
  }

  private byte[] getTrustStore() {
    final byte[] buffer = new byte[1024 * 1024];
    try {
      final FileInputStream input = new FileInputStream("/export/home/azkaban/EI_cacerts");
      final BufferedInputStream bufferedStream = new BufferedInputStream(input);
      final int size = bufferedStream.read(buffer);
      logger.info("Read bytes for " + ", size:" + size);
      return Arrays.copyOfRange(buffer, 0, size);

    } catch (final IOException e) {
      logger.error("one IO Exception.", e);
    }
    return null;
  }
}
