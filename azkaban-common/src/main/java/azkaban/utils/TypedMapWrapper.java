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

package azkaban.utils;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TypedMapWrapper<K, V> {

  private final Map<K, V> map;

  public TypedMapWrapper(final Map<K, V> map) {
    this.map = map;
  }

  public String getString(final K key) {
    return getString(key, null);
  }

  public String getString(final K key, final String defaultVal) {
    final Object obj = this.map.get(key);
    if (obj == null) {
      return defaultVal;
    }
    if (obj instanceof String) {
      return (String) obj;
    }

    return obj.toString();
  }

  public Boolean getBool(final K key, final Boolean defaultVal) {
    final Object obj = this.map.get(key);
    if (obj == null) {
      return defaultVal;
    }

    return (Boolean) obj;
  }

  public Integer getInt(final K key) {
    return getInt(key, -1);
  }

  public Integer getInt(final K key, final Integer defaultVal) {
    final Object obj = this.map.get(key);
    if (obj == null) {
      return defaultVal;
    }
    if (obj instanceof Integer) {
      return (Integer) obj;
    } else if (obj instanceof String) {
      return Integer.valueOf((String) obj);
    } else {
      return defaultVal;
    }
  }

  public Long getLong(final K key) {
    return getLong(key, -1L);
  }

  public Long getLong(final K key, final Long defaultVal) {
    final Object obj = this.map.get(key);
    if (obj == null) {
      return defaultVal;
    }
    if (obj instanceof Long) {
      return (Long) obj;
    } else if (obj instanceof Integer) {
      return Long.valueOf((Integer) obj);
    } else if (obj instanceof String) {
      return Long.valueOf((String) obj);
    } else {
      return defaultVal;
    }
  }

  public Double getDouble(final K key) {
    return getDouble(key, -1.0d);
  }

  public Double getDouble(final K key, final Double defaultVal) {
    final Object obj = this.map.get(key);
    if (obj == null) {
      return defaultVal;
    }
    if (obj instanceof Double) {
      return (Double) obj;
    } else if (obj instanceof String) {
      return Double.valueOf((String) obj);
    } else {
      return defaultVal;
    }
  }

  public Collection<String> getStringCollection(final K key) {
    final Object obj = this.map.get(key);
    return (Collection<String>) obj;
  }

  public Collection<String> getStringCollection(final K key,
      final Collection<String> defaultVal) {
    final Object obj = this.map.get(key);
    if (obj == null) {
      return defaultVal;
    }

    return (Collection<String>) obj;
  }

  public <C> Collection<C> getCollection(final K key) {
    final Object obj = this.map.get(key);
    if (obj instanceof Collection) {
      return (Collection<C>) obj;
    }
    return null;
  }

  public <L> List<L> getList(final K key) {
    final Object obj = this.map.get(key);
    if (obj instanceof List) {
      return (List<L>) obj;
    }
    return null;
  }

  public <L> List<L> getList(final K key, final List<L> defaultVal) {
    final Object obj = this.map.get(key);
    if (obj instanceof List) {
      return (List<L>) obj;
    }
    return defaultVal;
  }

  public Object getObject(final K key) {
    return this.map.get(key);
  }

  public Map<K, V> getMap() {
    return this.map;
  }

  public <S, T> Map<S, T> getMap(final K key) {
    return (Map<S, T>) this.map.get(key);
  }

  public boolean containsKey(final K key) {
    return this.map.containsKey(key);
  }
}
