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

import azkaban.imagemgmt.utils.CaseInsensitiveKeyDeserializers;
import java.io.IOException;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import org.codehaus.jackson.map.DeserializerProvider;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.deser.StdDeserializerProvider;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * VersionSet is defined as the set of key-val pairs to uniquely define the execution environment.
 * This version set will have azkaban version, azkaban configuration version, job type versions,
 * etc.
 * <p>
 * Example version set json: {"azConf":"0.0.5","azkaban":"0.0.2","sparkJobType":"0.1.2"}
 */
public class VersionSet {

  private final String versionSetJsonString;
  private final String versionSetMd5Hex;
  private final int versionSetId;
  private final SortedMap<String, VersionInfo> imageToVersionMap;
  private static final Logger logger = LoggerFactory
      .getLogger(VersionSet.class);

  /**
   * Constructor
   *
   * @param versionSetJsonString
   * @param versionSetMd5Hex
   * @param versionSetId
   */
  public VersionSet(final String versionSetJsonString, final String versionSetMd5Hex,
      final int versionSetId)
      throws IOException {
    this.versionSetJsonString = versionSetJsonString;
    this.versionSetMd5Hex = versionSetMd5Hex;
    this.versionSetId = versionSetId;
    final ObjectMapper mapper = new ObjectMapper();
    try {
      // Apply case insensitive key deserializers
      final DeserializerProvider provider = new StdDeserializerProvider()
          .withAdditionalKeyDeserializers(new CaseInsensitiveKeyDeserializers());
      mapper.setDeserializerProvider(provider);
      this.imageToVersionMap = mapper.readValue(this.versionSetJsonString,
          new TypeReference<TreeMap<String, VersionInfo>>() {
          });
    } catch (final Exception e) {
      throw new IOException(
          "Trouble converting Json string: " + this.versionSetJsonString + " to a TreeMap type", e);
    }
    logger.debug("Created version set with id: {}, md5: {}, json: {}",
        versionSetId, versionSetMd5Hex, versionSetJsonString);
  }

  /**
   * @return
   */
  public String getVersionSetJsonString() {
    return this.versionSetJsonString;
  }

  /**
   * @return
   */
  public SortedMap<String, VersionInfo> getImageToVersionMap() {
    return this.imageToVersionMap;
  }

  /**
   * @param imageType
   * @return
   */
  public Optional<VersionInfo> getVersion(final String imageType) {
    return Optional.ofNullable(this.imageToVersionMap.get(imageType.toLowerCase()));
  }

  /**
   * @return
   */
  public String getVersionSetMd5Hex() {
    return this.versionSetMd5Hex;
  }

  /**
   * @return
   */
  public int getVersionSetId() {
    return this.versionSetId;
  }
}
