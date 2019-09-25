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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


/**
 * Template Base Class to be capable to refresh Map Items from source object
 */
public class BaseRefreshableMap<K, V extends IRefreshable> extends HashMap<K, V>
    implements IRefreshable<BaseRefreshableMap<K, V>> {

  public BaseRefreshableMap<K, V> add(K key, V ramp) {
    this.put(key, ramp);
    return this;
  }

  public BaseRefreshableMap<K, V> delete(K id) {
    this.remove(id);
    return this;
  }

  @Override
  public BaseRefreshableMap<K, V> refresh(BaseRefreshableMap<K, V> source) {
    Set<K> mergedKeys = new HashSet();
    mergedKeys.addAll(this.keySet());
    mergedKeys.addAll(source.keySet());

    mergedKeys.stream().forEach(key -> {
      if (this.containsKey(key)) {
        if (source.containsKey(key)) {
          this.get(key).refresh(source.get(key));
        } else {
          this.remove(key);
        }
      } else {
        this.add(key, source.get(key));
      }
    });
    return this;
  }

  @Override
  public BaseRefreshableMap<K, V> clone() {
    return (BaseRefreshableMap<K, V>) super.clone();
  }
}
