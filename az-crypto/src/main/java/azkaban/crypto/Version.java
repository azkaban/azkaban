/*
 * Copyright (C) 2018 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package azkaban.crypto;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public enum Version {
  V1_0("1.0"),
  V1_1("1.1");

  private static final Map<String, Version> REVERSE_ENTRIES;

  static {
    final Builder<String, Version> builder = ImmutableMap.builder();
    for (final Version version : Version.values()) {
      builder.put(version._ver, version);
    }
    REVERSE_ENTRIES = builder.build();
  }

  private final String _ver;

  private Version(final String ver) {
    this._ver = ver;
  }

  /**
   * Provides Version enum based on version String
   *
   * @param ver Version String
   */
  public static Version fromVerString(final String ver) {
    final Version result = REVERSE_ENTRIES.get(ver);
    Preconditions.checkNotNull(ver, "Invalid version " + ver);
    return result;
  }

  /**
   * @return Naturally ordered list of version String.
   */
  public static List<String> versionStrings() {
    final List<String> versions = Lists.newArrayList(REVERSE_ENTRIES.keySet());
    Collections.sort(versions);
    return versions;
  }

  /**
   * @return Version string
   */
  public String versionStr() {
    return this._ver;
  }
}
