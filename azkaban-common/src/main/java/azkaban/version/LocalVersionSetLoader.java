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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@VisibleForTesting
public class LocalVersionSetLoader implements VersionSetLoader {
    private Map<String, Integer> versionSetMapping = new HashMap<>();
    private int max = 0;

    @Override
    public int insertAndGetVersionSetNum(String versionSetJsonString, String versionSetMd5Hex) throws IOException {
        this.versionSetMapping.put(versionSetMd5Hex, ++max);
        return this.versionSetMapping.get(versionSetMd5Hex);
    }

    @Override
    public boolean removeVersionSet(String versionSetMd5Hex) throws IOException {
        if (!this.versionSetMapping.containsKey(versionSetMd5Hex)) {
            // default to true
            return true;
        }
        return this.versionSetMapping.remove(versionSetMd5Hex, this.versionSetMapping.get(versionSetMd5Hex));
    }

    @Override
    public int getVersionSetNum(String versionSetJsonString, String versionSetMd5Hex) throws IOException {
        if (this.versionSetMapping.containsKey(versionSetMd5Hex)) {
            return this.versionSetMapping.get(versionSetMd5Hex);
        }
        return this.insertAndGetVersionSetNum(versionSetJsonString, versionSetMd5Hex);
    }
}
