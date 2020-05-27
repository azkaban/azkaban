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

import azkaban.utils.Pair;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

/**
 * Records the sequence of nodes and dag status change.
 */
class StatusChangeRecorder {

  private final List<Pair<String, Status>> sequence = new ArrayList<>();

  void recordNode(final Node node) {
    this.sequence.add(new Pair<>(node.getName(), node.getStatus()));
  }

  void recordDag(final Dag dag) {
    this.sequence.add(new Pair<>(dag.getName(), dag.getStatus()));
  }

  void verifySequence(final List<Pair<String, Status>> expectedSequence) {
    assertThat(this.sequence).isEqualTo(expectedSequence);
  }
}
