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

package azkaban.version;

import java.io.IOException;

public interface VersionSetLoader {

    /**
     * @return versionSetNum after created post insert
     */
    int insertAndGetVersionSetNum(String versionSetJsonString, String versionSetMd5Hex) throws IOException;

    /**
     * @return true if successful, otherwise false
     */
    boolean removeVersionSet(String versionSetMd5Hex) throws IOException;

    /**
     * @return versionSetNum corresponding to versionSetJsonString and versionSetMd5Hex
     */
    int getVersionSetNum(String versionSetJsonString, String versionSetMd5Hex) throws IOException;
}
