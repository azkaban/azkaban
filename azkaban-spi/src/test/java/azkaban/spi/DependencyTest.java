/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.spi;

import azkaban.test.executions.ThinArchiveTestUtils;
import azkaban.utils.InvalidHashException;
import java.io.File;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;


public class DependencyTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  @Test
  public void testCreateValidDependency() throws Exception {
    Dependency depA = ThinArchiveTestUtils.getDepA();
    Dependency newDep =
        new Dependency(depA.getFileName(), depA.getDestination(), depA.getType(), depA.getIvyCoordinates(), depA.getSHA1());

    assertEquals(depA, newDep);
  }

  @Test
  public void testCopyDependency() throws Exception {
    Dependency depA = ThinArchiveTestUtils.getDepA();
    Dependency newDep = depA.copy();

    assertEquals(depA, newDep);
  }

  @Test
  public void testMakeDependencyFile() throws Exception {
    Dependency depA = ThinArchiveTestUtils.getDepA();
    File file = TEMP_DIR.newFile(depA.getFileName());
    DependencyFile depFile = depA.makeDependencyFile(file);

    assertEquals(depA, depFile.copy());
    assertEquals(depFile.getFile(), file);
  }

  @Test(expected = InvalidHashException.class)
  public void testInvalidHash() throws Exception {
    Dependency depA = ThinArchiveTestUtils.getDepA();
    new Dependency(
            depA.getFileName(),
            depA.getDestination(),
            depA.getType(),
            depA.getIvyCoordinates(),
            "uh oh, I'm not a hash :(");
  }
}
