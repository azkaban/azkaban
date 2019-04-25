/*
 * Copyright (C) 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package azkaban.jobtype.javautils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import azkaban.flow.CommonJobProperties;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;


/**
 * Whitelist util. It uses file (new line separated) to construct whitelist and validates if id is
 * whitelisted.
 * Main use case is to control users onboarding on connector job types via their "user.to.proxy"
 * value.
 */
public class Whitelist {

  public static final String WHITE_LIST_FILE_PATH_KEY = "whitelist.file.path";
  private static final String PROXY_USER_KEY = "user.to.proxy";
  private static Logger logger = Logger.getLogger(Whitelist.class);

  private final Set<String> whitelistSet;

  /**
   * Creates whitelist instance.
   */
  public Whitelist(String whitelistFilePath, FileSystem fs) {
    this.whitelistSet = retrieveWhitelist(fs, new Path(whitelistFilePath));
    if (logger.isDebugEnabled()) {
      logger.debug("Whitelist: " + whitelistSet);
    }
  }

  public Whitelist(Props props, FileSystem fs) {
    this(props.getString(WHITE_LIST_FILE_PATH_KEY), fs);
  }

  /**
   * Checks if id is in whitelist.
   *
   * @throws UnsupportedOperationException if id is not whitelisted
   */
  public void validateWhitelisted(String id) {
    if (whitelistSet.contains(id)) {
      return;
    }
    throw new UnsupportedOperationException(id + " is not authorized");
  }

  /**
   * Use proxy user or submit user(if proxy user does not exist) from property and check if it is
   * whitelisted.
   */
  public void validateWhitelisted(Props props) {
    String id = null;
    if (props.containsKey(PROXY_USER_KEY)) {
      id = props.get(PROXY_USER_KEY);
      Preconditions.checkArgument(
          !StringUtils.isEmpty(id), PROXY_USER_KEY + " is required.");
    } else if (props.containsKey(CommonJobProperties.SUBMIT_USER)) {
      id = props.get(CommonJobProperties.SUBMIT_USER);
      Preconditions.checkArgument(!StringUtils.isEmpty(id),
          CommonJobProperties.SUBMIT_USER + " is required.");
    } else {
      throw new IllegalArgumentException(
          "Property neither has " + PROXY_USER_KEY + " nor " + CommonJobProperties.SUBMIT_USER);
    }
    validateWhitelisted(id);
  }

  /**
   * Updates whitelist if there's any change. If it needs to update whitelist, it enforces writelock
   * to make sure
   * there's an exclusive access on shared variables.
   */
  @VisibleForTesting
  Set<String> retrieveWhitelist(FileSystem fs, Path path) {
    try {
      Preconditions.checkArgument(
          fs.exists(path), "File does not exist at " + path
      );
      Preconditions.checkArgument(
          fs.isFile(path), "Whitelist path is not a file. " + path
      );

      Set<String> result = Sets.newHashSet();
      try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path),
          StandardCharsets.UTF_8))) {
        String s = null;
        while (!StringUtils.isEmpty((s = br.readLine()))) {
          result.add(s);
        }
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "Whitelist [whitelistSet=" + whitelistSet + "]";
  }
}
