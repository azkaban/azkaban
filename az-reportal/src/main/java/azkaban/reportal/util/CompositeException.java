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

package azkaban.reportal.util;

import java.util.List;

public class CompositeException extends Exception {
  private static final long serialVersionUID = 1L;
  private final List<? extends Exception> es;

  public CompositeException(final List<? extends Exception> es) {
    this.es = es;
  }

  public List<? extends Exception> getExceptions() {
    return this.es;
  }

  @Override
  public String toString() {
    final StringBuilder str = new StringBuilder();
    boolean pastFirst = false;

    for (final Exception e : this.es) {
      if (pastFirst) {
        str.append("\n\n");
      }
      str.append(e.toString());
      pastFirst = true;
    }

    return str.toString();
  }
}
