/*
 * Copyright 2014-2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.jobtype.connectors.gobblin;

import java.util.Map;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;


/**
 * An enum for GobblinPresets.
 * Gobblin has more than hundred properties and GobblinPresets represents set of default properties.
 * Using GobblinPresets, user can reduce number of input parameters which consequently increase
 * usability.
 */
public enum GobblinPresets {
  MYSQL_TO_HDFS("mysqlToHdfs"),
  HDFS_TO_MYSQL("hdfsToMysql");

  private static final Map<String, GobblinPresets> NAME_TO_PRESET;

  static {
    Map<String, GobblinPresets> tmp = Maps.newHashMap();
    for (GobblinPresets preset : GobblinPresets.values()) {
      tmp.put(preset.name, preset);
    }
    NAME_TO_PRESET = ImmutableMap.copyOf(tmp);
  }

  private final String name;

  GobblinPresets(String name) {
    this.name = name;
  }

  public static GobblinPresets fromName(String name) {
    GobblinPresets preset = NAME_TO_PRESET.get(name);
    if (preset == null) {
      throw new IllegalArgumentException(
          name + " is unrecognized. Known presets: " + NAME_TO_PRESET.keySet());
    }
    return preset;
  }
}
