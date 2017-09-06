/*
 * Copyright 2010 LinkedIn Corp.
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

package azkaban.utils;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A circular buffer of items of a given length. It will grow up to the give size as items are
 * appended, then it will begin to overwrite older items.
 *
 * @param <T> The type of the item contained.
 */
public class CircularBuffer<T> implements Iterable<T> {

  private final List<T> lines;
  private final int size;
  private int start;

  public CircularBuffer(final int size) {
    this.lines = new ArrayList<>();
    this.size = size;
    this.start = 0;
  }

  public void append(final T line) {
    if (this.lines.size() < this.size) {
      this.lines.add(line);
    } else {
      this.lines.set(this.start, line);
      this.start = (this.start + 1) % this.size;
    }
  }

  @Override
  public String toString() {
    return "[" + Joiner.on(", ").join(this.lines) + "]";
  }

  @Override
  public Iterator<T> iterator() {
    if (this.start == 0) {
      return this.lines.iterator();
    } else {
      return Iterators.concat(this.lines.subList(this.start, this.lines.size()).iterator(),
          this.lines.subList(0, this.start).iterator());
    }
  }

  public int getMaxSize() {
    return this.size;
  }

  public int getSize() {
    return this.lines.size();
  }

}
