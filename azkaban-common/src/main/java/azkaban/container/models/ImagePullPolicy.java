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

package azkaban.container.models;

public enum ImagePullPolicy {
    ALWAYS("Always"), IF_NOT_PRESENT("IfNotPresent"), NEVER("Never");

    private final String policyVal;

    ImagePullPolicy(final String policyVal) {
        this.policyVal = policyVal;
    }

    public String getPolicyVal() {
        return this.policyVal;
    }
}
