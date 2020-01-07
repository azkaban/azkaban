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
package azkaban.executor;

import com.sun.istack.NotNull;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Optional;
import java.util.Set;


/**
 * Object of Executable Ramp Exceptional Items
 */
public class ExecutableRampExceptionalItems implements IRefreshable<ExecutableRampExceptionalItems> {

  private volatile Hashtable<String, RampRecord> items = new Hashtable<>();

  private ExecutableRampExceptionalItems() {

  }

  public static ExecutableRampExceptionalItems createInstance() {
    return new ExecutableRampExceptionalItems();
  }

  public Hashtable<String, RampRecord> getItems() {
    return items;
  }

  public ExecutableRampExceptionalItems setItems(Hashtable<String, RampRecord> items) {
    this.items = items;
    return this;
  }

  public RampRecord get(final String key) {
    return this.items.get(key);
  }

  public ExecutableRampStatus getStatus(final String key) {
    return Optional.ofNullable(get(key)).map(RampRecord::getStatus).orElse(ExecutableRampStatus.UNDETERMINED);
  }

  public ExecutableRampExceptionalItems add(
      @NotNull final String key, @NotNull final ExecutableRampStatus treatment, final long timeStamp) {
    return add(key, treatment, timeStamp, false);
  }

  public ExecutableRampExceptionalItems add(
      @NotNull final String key, @NotNull final ExecutableRampStatus treatment, final long timeStamp, boolean isCacheOnly) {
    this.items.put(key, RampRecord.createInstance(treatment, timeStamp, isCacheOnly));
    return this;
  }

  @Override
  public ExecutableRampExceptionalItems refresh(ExecutableRampExceptionalItems source) {
    Set<String> mergedKeys = new HashSet();
    mergedKeys.addAll(this.items.keySet());
    mergedKeys.addAll(source.items.keySet());

    mergedKeys.stream().forEach(key -> {
      if (this.items.containsKey(key)) {
        if (source.items.containsKey(key)) {
          this.items.put(key, source.items.get(key));
        } else {
          this.items.remove(key);
        }
      } else {
        this.items.put(key, source.items.get(key));
      }
    });
    return this;
  }

  @Override
  public ExecutableRampExceptionalItems clone() {
    Hashtable<String, RampRecord> clonedItems = new Hashtable<>();
    clonedItems.putAll(this.getItems());

    return ExecutableRampExceptionalItems
        .createInstance()
        .setItems(clonedItems);
  }

  public static class RampRecord {
    private ExecutableRampStatus status;
    private long timeStamp;
    private boolean isCachedOnly = false;

    public RampRecord(ExecutableRampStatus status, long timeStamp, boolean isCachedOnly) {
      this.status = status;
      this.timeStamp = timeStamp;
      this.isCachedOnly = isCachedOnly;
    }

    public static RampRecord createInstance(ExecutableRampStatus status, long timeStamp) {
      RampRecord rampRecord = new RampRecord(status, timeStamp, false);
      return rampRecord;
    }

    public static RampRecord createInstance(ExecutableRampStatus status, long timeStamp, boolean isCachedOnly) {
      RampRecord rampRecord = new RampRecord(status, timeStamp, isCachedOnly);
      return rampRecord;
    }

    public RampRecord setCachedOnly() {
      this.isCachedOnly = true;
      return this;
    }

    public RampRecord resetCachedOnly() {
      this.isCachedOnly = false;
      return this;
    }

    public boolean isCachedOnly() {
      return this.isCachedOnly;
    }

    public ExecutableRampStatus getStatus() {
      return this.status;
    }

    public long getTimeStamp() {
      return this.timeStamp;
    }
  }
}
