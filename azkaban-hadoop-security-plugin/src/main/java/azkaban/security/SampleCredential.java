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

import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;

/**
 * This class serves as an example how we implement Credential. The actual class should be checked
 * in a separate jar (gradle product).
 */
public class SampleCredential implements Credential {

  // Sample secret key.
  private static final Text SECRET_KEY_NAME = new Text("azkaban.datavault.key");
  private final Credentials hadoopCrednetial;

  public SampleCredential(final Credentials hadoopCrednetial) {
    this.hadoopCrednetial = hadoopCrednetial;
  }

  @Override
  public void register(final String user) {
    this.hadoopCrednetial.addSecretKey(SECRET_KEY_NAME, getSecretKey(user));
  }

  private byte[] getSecretKey(final String user) {
    // would call out to service
    final Random r = new Random();
    return (String.valueOf(r.nextInt(10000)) + user).getBytes(StandardCharsets.UTF_8);
  }
}
