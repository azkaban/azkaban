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
import java.util.List;
import java.util.Optional;

/**
 * Interface for fetch, insert and deletion of VersionSet
 */
public interface VersionSetLoader {

  /**
   * @return versionSet after created post insert.
   */
  Optional<VersionSet> insertAndGetVersionSet(String versionSetMd5Hex, String versionSetJsonString)
      throws IOException;

  /**
   * @return true if successful, otherwise false.
   */
  boolean deleteVersionSet(String versionSetMd5Hex) throws IOException;

  /**
   * @return versionSet corresponding to versionSetJsonString and versionSetMd5Hex.
   */
  Optional<VersionSet> getVersionSet(String versionSetMd5Hex, String versionSetJsonString) throws IOException;

  /**
   * @return {@link VersionSet} corresponding to versionSetMd5Hex.
   */
  Optional<VersionSet> getVersionSet(String versionSetMd5Hex) throws IOException;

  /**
   * @return {@link VersionSet} corresponding to versionSetId.
   */
  Optional<VersionSet> getVersionSetById(int versionSetId) throws IOException;

  /**
   * @return List of all available {@link VersionSet}.
   */
  List<VersionSet> fetchAllVersionSets() throws IOException;
}
