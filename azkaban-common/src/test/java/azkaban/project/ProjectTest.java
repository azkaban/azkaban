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

package azkaban.project;

import org.junit.Test;

import static org.junit.Assert.*;

import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.utils.JSONUtils;

public class ProjectTest {
  @Test
  public void testToAndFromObject() throws Exception {
    Project project = new Project(1, "tesTing");
    project.setCreateTimestamp(1L);
    project.setLastModifiedTimestamp(2L);
    project.setDescription("I am a test");
    project.setUserPermission("user1", new Permission(new Type[] { Type.ADMIN,
        Type.EXECUTE }));

    Object obj = project.toObject();
    String json = JSONUtils.toJSON(obj);

    Object jsonObj = JSONUtils.parseJSONFromString(json);

    Project parsedProject = Project.projectFromObject(jsonObj);

    assertTrue(project.equals(parsedProject));
  }

}
