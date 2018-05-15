/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban.dag;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Test;

public class NodeTest {

  @Test
  public void hasParent() {
    final Node node = createTestNode("a");
    final Node parentNode = createTestNode("parent");
    node.addParent(parentNode);
    final boolean hasParent = node.hasParent();
    assertThat(hasParent).isTrue();
  }

  @Test
  public void hasParentNegative() {
    final Node node = createTestNode("a");
    final boolean hasParent = node.hasParent();
    assertThat(hasParent).isFalse();
  }

  private Node createTestNode(final String name) {
    return TestUtil.createNodeWithNullProcessor(name, mock(Dag.class));
  }


}
