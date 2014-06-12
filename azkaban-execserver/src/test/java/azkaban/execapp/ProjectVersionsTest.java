/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.execapp;

import java.util.ArrayList;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

public class ProjectVersionsTest {

  @Test
  public void testVersionOrdering() {
    ArrayList<ProjectVersion> pversion = new ArrayList<ProjectVersion>();
    pversion.add(new ProjectVersion(1, 2));
    pversion.add(new ProjectVersion(1, 3));
    pversion.add(new ProjectVersion(1, 1));

    Collections.sort(pversion);

    int i = 0;
    for (ProjectVersion version : pversion) {
      Assert.assertTrue(i < version.getVersion());
      i = version.getVersion();
    }
  }
}
