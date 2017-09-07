/*
 * Copyright 2017 LinkedIn Corp.
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

import org.junit.Test;

public class NodeTest {

  @Test
  public void hasParent() throws Exception {
    final Node node = new Node("a", null);
    final Node parentNode = new Node("parent", null);
    parentNode.addChild(node);
    final boolean hasParent = node.hasParent();
    assertThat(hasParent).isTrue();
  }

  @Test
  public void hasParentNegative() throws Exception {
    final Node node = new Node("a", null);
    final boolean hasParent = node.hasParent();
    assertThat(hasParent).isFalse();
  }

}
