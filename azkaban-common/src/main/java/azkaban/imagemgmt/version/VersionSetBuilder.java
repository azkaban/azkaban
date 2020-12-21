/*
 * Copyright 2020 LinkedIn Corp.
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

package azkaban.imagemgmt.version;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.codec.digest.DigestUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * This builder class is responsible for creating key-ordered json string for the version set, to
 * ensure that the same execution environment will have the same string and hence the md5 digest
 * will remain the same.
 */
public class VersionSetBuilder {

  private final Map<String, String> versionSetElements = new TreeMap();
  private final VersionSetLoader loader;

  public VersionSetBuilder(VersionSetLoader loader) {
    this.loader = loader;
  }

  public VersionSetBuilder addElement(String key, String version) {
    this.versionSetElements.put(key, version);
    return this;
  }

  public VersionSetBuilder addElements(Map<String, String> keyVals) {
    this.versionSetElements.putAll(keyVals);
    return this;
  }

  public VersionSet build() throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    String versionSetJsonString = objectMapper.writeValueAsString(versionSetElements);
    String versionSetMd5Hex = DigestUtils.md5Hex(versionSetJsonString);
    return this.loader.getVersionSet(versionSetMd5Hex, versionSetJsonString)
        .orElse(null);  // null implies Exception was thrown by the Dao Layer
  }
}
