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

import java.util.ArrayList;
import java.util.List;
import javafx.util.Pair;

/**
 * Records the sequence of nodes and flow status change.
 */
public class StatusChangeRecorder {

  List<Pair<String, Status>> sequence = new ArrayList<>();

  public void recordNode(final Node node) {
    this.sequence.add(new Pair(node.getName(), node.getStatus()));
  }

  public void recordFlow(final Flow flow) {
    this.sequence.add(new Pair(flow.getName(), flow.getStatus()));
  }

  public void verifySequence(final List<Pair<String, Status>> expectedSequence) {
    assertThat(this.sequence).isEqualTo(expectedSequence);
  }
}
