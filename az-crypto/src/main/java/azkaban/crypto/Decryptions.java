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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class Decryptions {

  private static final FsPermission USER_READ_PERMISSION_ONLY = new FsPermission(FsAction.READ,
      FsAction.NONE,
      FsAction.NONE);

  public String decrypt(final String cipheredText, final String passphrasePath, final FileSystem fs)
      throws IOException {
    Preconditions.checkNotNull(cipheredText);
    Preconditions.checkNotNull(passphrasePath);

    final Path path = new Path(passphrasePath);
    Preconditions.checkArgument(fs.exists(path), "File does not exist at " + passphrasePath);
    Preconditions
        .checkArgument(fs.isFile(path), "Passphrase path is not a file. " + passphrasePath);

    final FileStatus fileStatus = fs.getFileStatus(path);
    Preconditions.checkArgument(USER_READ_PERMISSION_ONLY.equals(fileStatus.getPermission()),
        "Passphrase file should only have read only permission on only user. " + passphrasePath);

    final Crypto crypto = new Crypto();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path),
        Charset.defaultCharset()))) {
      final String passphrase = br.readLine();
      final String decrypted = crypto.decrypt(cipheredText, passphrase);
      Preconditions.checkNotNull(decrypted, "Was not able to decrypt");
      return decrypted;
    }
  }
}